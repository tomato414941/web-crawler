"""Tests for output module."""

import json

import pytest

from crawler.output import StreamingOutputWriter


class TestStreamingOutputWriter:
    def test_creates_file_and_directory(self, tmp_path):
        """Should create output file and parent directories."""
        output_file = tmp_path / "subdir" / "output.jsonl"
        with StreamingOutputWriter(output_file) as writer:
            writer.write_one({"url": "http://example.com"})

        assert output_file.exists()

    def test_writes_jsonl_format(self, tmp_path):
        """Should write valid JSONL format."""
        output_file = tmp_path / "output.jsonl"
        with StreamingOutputWriter(output_file) as writer:
            writer.write_one({"url": "http://example.com/1"})
            writer.write_one({"url": "http://example.com/2"})

        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"url": "http://example.com/1"}
        assert json.loads(lines[1]) == {"url": "http://example.com/2"}

    def test_include_content_false_excludes_content(self, tmp_path):
        """include_content=False should exclude content field."""
        output_file = tmp_path / "output.jsonl"
        with StreamingOutputWriter(output_file, include_content=False) as writer:
            writer.write_one({"url": "http://example.com", "content": "Hello"})

        content = json.loads(output_file.read_text().strip())
        assert "content" not in content
        assert content["url"] == "http://example.com"

    def test_include_content_true_keeps_content(self, tmp_path):
        """include_content=True should keep content field."""
        output_file = tmp_path / "output.jsonl"
        with StreamingOutputWriter(output_file, include_content=True) as writer:
            writer.write_one({"url": "http://example.com", "content": "Hello"})

        content = json.loads(output_file.read_text().strip())
        assert content["content"] == "Hello"

    def test_japanese_and_special_characters(self, tmp_path):
        """Should handle Japanese and special characters correctly."""
        output_file = tmp_path / "output.jsonl"
        with StreamingOutputWriter(output_file) as writer:
            writer.write_one({"text": "Hello World"})

        content = output_file.read_text(encoding="utf-8").strip()
        # ensure_ascii=False should preserve unicode
        assert "Hello" in content
        data = json.loads(content)
        assert data["text"] == "Hello World"

    def test_count_property(self, tmp_path):
        """count property should return number of written results."""
        output_file = tmp_path / "output.jsonl"
        with StreamingOutputWriter(output_file) as writer:
            assert writer.count == 0
            writer.write_one({"url": "http://example.com/1"})
            assert writer.count == 1
            writer.write_one({"url": "http://example.com/2"})
            assert writer.count == 2

    def test_write_without_context_manager_raises(self, tmp_path):
        """Should raise RuntimeError if used without context manager."""
        output_file = tmp_path / "output.jsonl"
        writer = StreamingOutputWriter(output_file)
        with pytest.raises(RuntimeError):
            writer.write_one({"url": "http://example.com"})

    def test_preserves_other_fields(self, tmp_path):
        """Should preserve all other fields in the result."""
        output_file = tmp_path / "output.jsonl"
        result = {
            "url": "http://example.com",
            "status": 200,
            "depth": 1,
            "timestamp": 1234567890.0,
        }
        with StreamingOutputWriter(output_file) as writer:
            writer.write_one(result)

        content = json.loads(output_file.read_text().strip())
        assert content == result

    def test_does_not_modify_original_dict(self, tmp_path):
        """Should not modify the original result dictionary."""
        output_file = tmp_path / "output.jsonl"
        result = {"url": "http://example.com", "content": "Hello"}

        with StreamingOutputWriter(output_file, include_content=False) as writer:
            writer.write_one(result)

        # Original dict should still have content
        assert "content" in result
        assert result["content"] == "Hello"

    def test_flushes_after_each_write(self, tmp_path):
        """Should flush after each write for real-time updates."""
        output_file = tmp_path / "output.jsonl"
        with StreamingOutputWriter(output_file) as writer:
            writer.write_one({"url": "http://example.com/1"})
            # File should be readable immediately
            content = output_file.read_text()
            assert "example.com/1" in content
