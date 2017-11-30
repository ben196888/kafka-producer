const Kafka = require('node-rdkafka');

const producer = new Kafka.Producer({
    'client.id': 'kafka-producer-client',
    'metadata.broker.list': 'localhost:9092',
    'dr_cb': true,
});

let interval = null;

// Connect to the broker manually
producer.connect();

// Wait for the ready event before proceeding
producer.on('ready', function() {
    let count = 0;
    try {
        const opts = {
            topic: 'topic',
            timeout: 10000,
        };
        interval = setInterval(function() {
            producer.produce(
                'topic',
                null,
                new Buffer(`Awesome message ${count}`),
                'Stormwind',
                Date.now(),
            );
            count += 1;
        }, 1000);
    } catch (err) {
        console.error('A problem occurred when sending our message');
        console.error(err);
        clearInterval(interval);
    }
});

// Any errors we encounter, including connection errors
producer.on('event.error', function(err) {
    console.error('Error from producer');
    console.error(err);
});

producer.setPollInterval(500);

producer.on('delivery-report', function(err, report) {
    // Report of delivery statistics here:
    //
    console.log(report);
});

process.on('SIGINT', function() {
    console.log('Disconnect kafka...');
    clearInterval(interval);
    producer.disconnect();
    process.exit();
});
