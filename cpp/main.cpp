#include <iostream>
#include <string>
#include <vector>
#include <cmath>

#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// Kafka configuration
const std::string BROKERS = "kafka:9092";
const std::string INPUT_TOPIC = "pgserver1.public.orders";
const std::string OUTPUT_TOPIC = "processed-orders";
const std::string GROUP_ID = "cpp-processor-group";


// Kafka error callback
class KafkaEventCallback : public RdKafka::EventCb
{
public:

    void event_cb(RdKafka::Event& event) override
    {
        if (event.type() == RdKafka::Event::EVENT_ERROR)
        {
            std::cerr << "[Kafka ERROR] "
                << event.str()
                << std::endl;
        }
    }
};


// Process one Debezium event
bool processEvent(
    const std::string& inputMessage,
    RdKafka::Producer* producer)
{
    try
    {
        json event = json::parse(inputMessage);

        // Check that the Debezium event contains data
        if (!event.contains("payload") ||
            !event["payload"].contains("after") ||
            event["payload"]["after"].is_null())
        {
            std::cout << "No 'after' data. Skipping event.\n";
            return false;
        }

        json after = event["payload"]["after"];

        int id = after["id"];
        std::string customerName = after["customer_name"];
        double amount = after["amount"];

        // C++ stream processing
        double processedAmount =
            std::round(amount * 1.10 * 100.0) / 100.0;

        // Create output message
        json output;

        output["id"] = id;
        output["customer_name"] = customerName;
        output["original_amount"] = amount;
        output["processed_amount"] = processedAmount;

        std::string outputMessage = output.dump();

        std::cout << "Processed by C++:\n";
        std::cout << outputMessage << "\n";

        // Use database ID as Kafka message key
        std::string key = std::to_string(id);

        RdKafka::ErrorCode result =
            producer->produce(
                OUTPUT_TOPIC,
                RdKafka::Topic::PARTITION_UA,
                RdKafka::Producer::RK_MSG_COPY,
                const_cast<char*>(outputMessage.c_str()),
                outputMessage.size(),
                key.c_str(),
                key.size(),
                0,
                nullptr
            );

        if (result != RdKafka::ERR_NO_ERROR)
        {
            std::cerr << "Failed to produce message: "
                << RdKafka::err2str(result)
                << std::endl;

            return false;
        }

        producer->poll(0);

        std::cout << "Sent to topic: "
            << OUTPUT_TOPIC
            << "\n";

        return true;
    }
    catch (const std::exception& e)
    {
        std::cerr << "JSON processing error: "
            << e.what()
            << std::endl;

        return false;
    }
}


int main()
{
    // Flush console output immediately
    std::cout.setf(std::ios::unitbuf);

    std::cout << "=====================================\n";
    std::cout << "      C++ Kafka Stream Processor\n";
    std::cout << "=====================================\n";
    std::cout << "Input topic : " << INPUT_TOPIC << "\n";
    std::cout << "Output topic: " << OUTPUT_TOPIC << "\n";
    std::cout << "=====================================\n";

    std::string error;

    // =====================================
    // Create Kafka consumer
    // =====================================

    RdKafka::Conf* consumerConf =
        RdKafka::Conf::create(
            RdKafka::Conf::CONF_GLOBAL);

    consumerConf->set(
        "bootstrap.servers",
        BROKERS,
        error);

    consumerConf->set(
        "group.id",
        GROUP_ID,
        error);

    consumerConf->set(
        "auto.offset.reset",
        "earliest",
        error);

    KafkaEventCallback eventCallback;

    consumerConf->set(
        "event_cb",
        &eventCallback,
        error);

    RdKafka::KafkaConsumer* consumer =
        RdKafka::KafkaConsumer::create(
            consumerConf,
            error);

    if (!consumer)
    {
        std::cerr << "Failed to create consumer: "
            << error
            << std::endl;

        delete consumerConf;
        return 1;
    }

    delete consumerConf;


    // =====================================
    // Subscribe to input topic
    // =====================================

    std::vector<std::string> topics;
    topics.push_back(INPUT_TOPIC);

    RdKafka::ErrorCode subscribeResult =
        consumer->subscribe(topics);

    if (subscribeResult != RdKafka::ERR_NO_ERROR)
    {
        std::cerr << "Failed to subscribe: "
            << RdKafka::err2str(subscribeResult)
            << std::endl;

        delete consumer;
        return 1;
    }


    // =====================================
    // Create Kafka producer
    // =====================================

    RdKafka::Conf* producerConf =
        RdKafka::Conf::create(
            RdKafka::Conf::CONF_GLOBAL);

    producerConf->set(
        "bootstrap.servers",
        BROKERS,
        error);

    RdKafka::Producer* producer =
        RdKafka::Producer::create(
            producerConf,
            error);

    if (!producer)
    {
        std::cerr << "Failed to create producer: "
            << error
            << std::endl;

        delete producerConf;
        delete consumer;
        return 1;
    }

    delete producerConf;


    // =====================================
    // Start stream processing
    // =====================================

    std::cout << "C++ stream processor is running...\n";
    std::cout << "Waiting for Kafka messages...\n\n";


    while (true)
    {
        RdKafka::Message* message =
            consumer->consume(1000);

        // No message arrived
        if (message->err() == RdKafka::ERR__TIMED_OUT)
        {
            delete message;
            continue;
        }

        // Kafka error
        if (message->err() != RdKafka::ERR_NO_ERROR)
        {
            std::cerr << "Consumer error: "
                << message->errstr()
                << std::endl;

            delete message;
            continue;
        }


        // =====================================
        // CONSUME
        // =====================================

        std::string inputMessage(
            static_cast<const char*>(
                message->payload()),
            message->len()
        );

        std::cout << "-------------------------------------\n";
        std::cout << "Received from Kafka:\n";
        std::cout << inputMessage << "\n\n";


        // =====================================
        // PROCESS + PRODUCE
        // =====================================

        processEvent(
            inputMessage,
            producer);


        delete message;
    }


    // Cleanup
    consumer->close();

    producer->flush(5000);

    delete producer;
    delete consumer;

    return 0;
}