from confluent_kafka import Producer

def test():
    conf = {'bootstrap.servers': 'broker:29092'}
    p = Producer(conf)

    def delivery_report(err, msg):
        if err:
            print(f'ERROR: {err}')
        else:
            print(f'Message sent to {msg.topic()} [{msg.partition()}] @ {msg.offset()}')

    p.produce('your_topic', value='test message', callback=delivery_report)
    p.flush()

    print('hello world')