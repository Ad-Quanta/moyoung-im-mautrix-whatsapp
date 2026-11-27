package wamedia

import (
	"bytes"
	"image"
	"image/jpeg"
	"image/png"
	"testing"
)

// Inbox 的媒体转换无需加载 Matrix 转换器也能解码 PNG 并保留图片尺寸。
func TestConvertImageToJPEG(t *testing.T) {
	var source bytes.Buffer
	if err := png.Encode(&source, image.NewRGBA(image.Rect(0, 0, 4, 3))); err != nil {
		t.Fatal(err)
	}
	data, err := ConvertImageToJPEG(source.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	converted, err := jpeg.Decode(bytes.NewReader(data))
	if err != nil || converted.Bounds() != image.Rect(0, 0, 4, 3) {
		t.Fatalf("unexpected converted image %v, err=%v", converted, err)
	}
}
