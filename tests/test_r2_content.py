import gzip
from io import BytesIO

from crawler.r2_content import R2ContentStore


class FakeS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], dict[str, object]] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType, ContentEncoding=None):
        self.objects[(Bucket, Key)] = {
            "Body": Body,
            "ContentType": ContentType,
            "ContentEncoding": ContentEncoding,
        }

    def get_object(self, *, Bucket, Key):
        stored = self.objects[(Bucket, Key)]
        response = {"Body": BytesIO(stored["Body"]), "ContentType": stored["ContentType"]}
        if stored["ContentEncoding"]:
            response["ContentEncoding"] = stored["ContentEncoding"]
        return response

    def delete_object(self, *, Bucket, Key):
        self.objects.pop((Bucket, Key), None)


def test_put_get_and_delete_page_body():
    client = FakeS3Client()
    store = R2ContentStore(client, "web-crawler-content")

    store.put("abc123", b"<html>raw</html>", "text/html")

    assert store.get("abc123") == b"<html>raw</html>"
    assert client.objects[("web-crawler-content", "abc123")]["ContentType"] == "text/html"

    store.delete("abc123")
    assert client.objects == {}


def test_put_stores_page_body_gzip_compressed():
    client = FakeS3Client()
    store = R2ContentStore(client, "web-crawler-content")
    body = b"<html>" + b"<p>repeated text</p>" * 500 + b"</html>"

    store.put("abc123", body, "text/html")

    stored = client.objects[("web-crawler-content", "abc123")]
    assert stored["ContentEncoding"] == "gzip"
    assert len(stored["Body"]) < len(body)
    assert gzip.decompress(stored["Body"]) == body


def test_get_reads_page_body_stored_without_compression():
    client = FakeS3Client()
    client.put_object(
        Bucket="web-crawler-content",
        Key="abc123",
        Body=b"<html>raw</html>",
        ContentType="text/html",
    )
    store = R2ContentStore(client, "web-crawler-content")

    assert store.get("abc123") == b"<html>raw</html>"
