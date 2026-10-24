import gzip
import lzma
from io import BytesIO

from crawler.r2_content import R2ContentStore, compress_page_body, new_bundle_key


class FakeS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], dict[str, object]] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType, ContentEncoding=None):
        self.objects[(Bucket, Key)] = {
            "Body": Body,
            "ContentType": ContentType,
            "ContentEncoding": ContentEncoding,
        }

    def get_object(self, *, Bucket, Key, Range=None):
        stored = self.objects[(Bucket, Key)]
        body = stored["Body"]
        if Range is not None:
            start, end = Range.removeprefix("bytes=").split("-")
            body = body[int(start) : int(end) + 1]
        response = {"Body": BytesIO(body), "ContentType": stored["ContentType"]}
        if stored["ContentEncoding"]:
            response["ContentEncoding"] = stored["ContentEncoding"]
        return response

    def delete_object(self, *, Bucket, Key):
        self.objects.pop((Bucket, Key), None)


def test_compress_page_body_shrinks_html_and_round_trips():
    body = b"<html>" + b"<p>repeated text</p>" * 500 + b"</html>"

    record = compress_page_body(body)

    assert len(record) < len(body)
    assert lzma.decompress(record) == body


def test_bundle_records_are_read_back_one_page_at_a_time():
    client = FakeS3Client()
    store = R2ContentStore(client, "web-crawler-content")
    first = compress_page_body(b"<html>first</html>")
    second = compress_page_body(b"<html>second</html>")

    store.put_bundle("bundles/one", first + second)

    assert store.get_record("bundles/one", 0, len(first)) == b"<html>first</html>"
    assert store.get_record("bundles/one", len(first), len(second)) == b"<html>second</html>"
    assert len(client.objects) == 1


def test_new_bundle_keys_are_unique():
    keys = {new_bundle_key() for _ in range(100)}

    assert len(keys) == 100
    assert all(key.startswith("bundles/") for key in keys)


def test_get_reads_gzip_page_body_stored_as_its_own_object():
    client = FakeS3Client()
    client.put_object(
        Bucket="web-crawler-content",
        Key="abc123",
        Body=gzip.compress(b"<html>raw</html>"),
        ContentType="text/html",
        ContentEncoding="gzip",
    )
    store = R2ContentStore(client, "web-crawler-content")

    assert store.get("abc123") == b"<html>raw</html>"


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


def test_delete_removes_object():
    client = FakeS3Client()
    store = R2ContentStore(client, "web-crawler-content")
    store.put_bundle("bundles/one", compress_page_body(b"<html>raw</html>"))

    store.delete("bundles/one")

    assert client.objects == {}
