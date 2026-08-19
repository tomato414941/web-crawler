from io import BytesIO

from crawler.r2_content import R2ContentStore


class FakeS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], tuple[bytes, str]] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType):
        self.objects[(Bucket, Key)] = (Body, ContentType)

    def get_object(self, *, Bucket, Key):
        body, _content_type = self.objects[(Bucket, Key)]
        return {"Body": BytesIO(body)}

    def delete_object(self, *, Bucket, Key):
        self.objects.pop((Bucket, Key), None)


def test_put_get_and_delete_page_body():
    client = FakeS3Client()
    store = R2ContentStore(client, "web-crawler-content")

    store.put("abc123", b"<html>raw</html>", "text/html")

    assert store.get("abc123") == b"<html>raw</html>"
    assert client.objects[("web-crawler-content", "abc123")][1] == "text/html"

    store.delete("abc123")
    assert client.objects == {}
