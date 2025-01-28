
kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}

window_config = {
    "window_duration": "1 minute",       # Довжина вікна
    "sliding_interval": "30 seconds",   # Інтервал зсуву вікна
    "watermark_duration": "10 seconds"  # Тривалість watermark
}