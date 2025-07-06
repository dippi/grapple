# logadmin

This folder contains a trimmed and edited copy of [`cloud.google.com/go/logging/logadmin`](https://github.com/googleapis/google-cloud-go/blob/logging/v1.13.0/logging/logadmin/logadmin.go).

The changes enable us to directly iterate over `loggingpb.LogEntry` instead of `logging.Entry`. This adjustment ensures that log entries are serialized to match the output format of `gcloud logging read`.
