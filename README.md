# s3-elb-access-logs-to-cloudwatch

Access logs in S3 aren't very useful. Copy them to cloudwatch logs! This is a standalone script but could be used as a lambda triggerin on ObjectCreated for your bucket.

This script automatically detects alb vs. elb logs and formats them appropriately as json.

**USAGE: elb_access_to_cloudwatch.py [log_group_name] [bucket_name] [remaining_path]**

e.g. if your log group is my-group and your bucket is my-bucket and the log file is at s3://my-bucket/elb/2020/11/8/log.gz

```
./elb_access_to_cloudwatch.py my-group my-bucket elb/2020/11/8/log.gz
```

- It expects that your log group exists already
- It names the log stream after the s3 object/file name
- It chunks to multiple log streams if the s3 objects is big
