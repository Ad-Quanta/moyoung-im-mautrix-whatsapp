package wamedia

import (
	"bytes"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png" // 注册 PNG 解码器，支持 Router 上传的图片。

	_ "golang.org/x/image/webp" // 注册 WebP 解码器，支持直接转成 JPEG。
)

// ConvertImageToJPEG 将 Router 支持的 PNG 或 WebP 转成 WhatsApp 发送所需的 JPEG。
func ConvertImageToJPEG(data []byte) ([]byte, error) {
	decoded, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to decode image: %w", err)
	}

	var jpgBuffer bytes.Buffer
	if err = jpeg.Encode(&jpgBuffer, decoded, &jpeg.Options{Quality: 80}); err != nil {
		return nil, fmt.Errorf("failed to encode JPEG image: %w", err)
	}

	return jpgBuffer.Bytes(), nil
}
