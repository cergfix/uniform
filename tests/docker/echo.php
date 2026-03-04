<?php
header("X-Echo: ok");
echo json_encode([
  "method" => $_SERVER["REQUEST_METHOD"] ?? "UNKNOWN",
  "body" => file_get_contents("php://input"),
  "script" => $_SERVER["SCRIPT_FILENAME"] ?? "",
]);
