package connector

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func isHEICImage(mimeType, fileName string) bool {
	mimeType, _, _ = strings.Cut(strings.ToLower(mimeType), ";")
	switch strings.TrimSpace(mimeType) {
	case "image/heic", "image/heif", "image/heic-sequence", "image/heif-sequence":
		return true
	}
	extension := strings.ToLower(filepath.Ext(fileName))
	return extension == ".heic" || extension == ".heif"
}

func jpegFileName(fileName string) string {
	if fileName == "" {
		return "image.jpg"
	}
	return strings.TrimSuffix(fileName, filepath.Ext(fileName)) + ".jpg"
}

func convertHEICToJPEG(ctx context.Context, imageData []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "chatgpt-dots-heic-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary HEIC directory: %w", err)
	}
	defer os.RemoveAll(dir)

	inputPath := filepath.Join(dir, "input.heic")
	if err = os.WriteFile(inputPath, imageData, 0o600); err != nil {
		return nil, fmt.Errorf("failed to write temporary HEIC file: %w", err)
	}

	// iPhone photos are HEIF grids of tiles. The ffmpeg CLI only decodes the
	// first tile, while libheif assembles the full image.
	jpegData, err := runHEIFDec(ctx, dir, inputPath, "jpg")
	if err == nil {
		return stripJPEGMetadata(jpegData), nil
	} else if ctx.Err() != nil {
		return nil, err
	}

	// libheif's JPEG writer gives up on a broken EXIF block, while its PNG
	// writer skips the block and writes the image.
	pngData, pngErr := runHEIFDec(ctx, dir, inputPath, "png")
	if pngErr != nil {
		return nil, err
	}
	img, imgErr := png.Decode(bytes.NewReader(pngData))
	if imgErr != nil {
		return nil, fmt.Errorf("failed to decode converted PNG: %w", imgErr)
	}
	var buf bytes.Buffer
	if err = jpeg.Encode(&buf, img, &jpeg.Options{Quality: 90}); err != nil {
		return nil, fmt.Errorf("failed to encode converted image: %w", err)
	}
	return buf.Bytes(), nil
}

func runHEIFDec(ctx context.Context, dir, inputPath, extension string) ([]byte, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "heif-dec", "--quiet", inputPath, filepath.Join(dir, "output."+extension))
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("heif-dec failed: %w", ctxErr)
		}
		return nil, fmt.Errorf("heif-dec failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	// heif-dec writes output-1.jpg, output-2.jpg, ... instead of output.jpg
	// when the file holds several top-level images. Use the first one.
	outputs, err := filepath.Glob(filepath.Join(dir, "output*."+extension))
	if err != nil || len(outputs) == 0 {
		return nil, fmt.Errorf("heif-dec produced no output: %s", strings.TrimSpace(stderr.String()))
	}
	data, err := os.ReadFile(outputs[0])
	if err != nil {
		return nil, fmt.Errorf("failed to read converted image: %w", err)
	}
	// heif-dec exits 0 even when its writer fails, leaving an empty file.
	config, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || config.Width <= 0 || config.Height <= 0 {
		return nil, fmt.Errorf("heif-dec produced invalid %s output: %s", extension, strings.TrimSpace(stderr.String()))
	}
	return data, nil
}

// stripJPEGMetadata drops every APP1 segment. heif-dec copies the HEIC's EXIF
// (split over several APP1 segments when large) and XMP into them, including
// GPS location and device model. Rotation is already applied to the pixels.
func stripJPEGMetadata(data []byte) []byte {
	if len(data) < 4 || data[0] != 0xFF || data[1] != 0xD8 {
		return data
	}
	out := make([]byte, 0, len(data))
	out = append(out, data[:2]...)
	pos := 2
	for pos+4 <= len(data) && data[pos] == 0xFF {
		marker := data[pos+1]
		if marker == 0xDA {
			break
		}
		length := int(data[pos+2])<<8 | int(data[pos+3])
		end := pos + 2 + length
		if length < 2 || end > len(data) {
			return data
		}
		if marker != 0xE1 {
			out = append(out, data[pos:end]...)
		}
		pos = end
	}
	return append(out, data[pos:]...)
}
