name = "chatcloudnew"
main = "index.js"
compatibility_date = "2026-04-14"

[[durable_objects.bindings]]
name = "CHAT_SERVER_3"
class_name = "ChatServer3"

[[migrations]]
tag = "v1"
new_classes = ["ChatServer3"]

