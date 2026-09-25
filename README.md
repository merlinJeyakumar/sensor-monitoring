# Sensor Monitoring

use Jdk 21 and run example.monitoring.MainKt in two configurations, first with program argument central then with warehouse and environment variable WAREHOUSE_ID=warehouse-a. Keep both running to receive sensor readings and display alarms.


## Send a reading using shell

``` shell
$client = [System.Net.Sockets.UdpClient]::new()
try {
    $payload = [System.Text.Encoding]::UTF8.GetBytes('sensor_id=t1; value=36')
    [void]$client.Send($payload, $payload.Length, '127.0.0.1', 3344)
} finally {
    $client.Dispose()
}
```

Akka Url need to be configured at gradle.properties/akkaRepositoryUrl
