from xml_aws_athena.cloud import Storage

s3 = Storage()


def test_create_bucket():
    # Call the function with a bucket name and region
    bucket_name = "teste-mvsh888"
    result = s3.create_bucket(bucket_name)

    # Assert that the function returns True
    assert result is True


def test_put_object_file_test():
    # Call the function with a bucket name and object key
    bucket_name = "teste-mvsh888"

    result = []
    for p in range(10):
        body = f"This is a test file. {p}"
        object_key = f"test_{p}.txt"
        # Call the function with a BytesIO object
        rst = s3.put_object_file(body, bucket_name, object_key)
        result.append(rst)

    # Assert that the function does not raise an exception
    assert all(result) is True


def test_delete_bucket():
    # Call the function with a bucket name
    bucket_name = "teste-mvsh888"
    result = s3.delete_bucket_objects(bucket_name)

    # Assert that the function returns True
    assert result is True
