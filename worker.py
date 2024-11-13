import json
import pika
from xprint import xprint
import json
import time
from db_and_event_definitions import CUSTOMERS_DATABASE, RIDES_DATABASE, TicketEvent, CustomerEvent
from xprint import xprint


class TicketWorker:

    def __init__(self, worker_id):
        # Do not edit the init method.
        # Set the variables appropriately in the methods below.
        self.connection = None
        self.channel = None
        self.worker_id = worker_id
        self.ticket_event_exchange = "ticket_events_exchange"
        self.queue = "ticket_event"
        self.dead_letter_exchange = "ticket_events_dead_letter_exchange"
        self.dead_letter_queue = "ticket_events_dead_letter_queue"
        self.dead_letter_routing_key = "ticket_events_dead_letter_routing_key"
        self.ticket_events = []
        self.customer_event_producer = None

    def initialize_rabbitmq(self):
        # To implement - Initialize the RabbitMQ connection, channel, exchanges and queues here
        # Also initialize the customer_event_producer used for publishing customer events
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        # Declare ticket_events_exchange as direct exchange
        self.channel.exchange_declare(exchange=self.ticket_event_exchange, exchange_type='direct')

        # Declare the main queue with dead-letter exchange settings
        self.channel.queue_declare(queue=self.queue, arguments={
            'x-dead-letter-exchange': self.dead_letter_exchange,
            'x-dead-letter-routing-key': self.dead_letter_routing_key
        })

        # Declare dead letter exchange and queue
        self.channel.exchange_declare(exchange=self.dead_letter_exchange, exchange_type='direct')
        self.channel.queue_declare(queue=self.dead_letter_queue)
        self.channel.queue_bind(queue=self.dead_letter_queue, exchange=self.dead_letter_exchange,
                                routing_key=self.dead_letter_routing_key)

        # Bind main queue to ticket_events_exchange
        self.channel.queue_bind(queue=self.queue, exchange=self.ticket_event_exchange, routing_key='ticket_event')

        # Initialize CustomerEventProducer
        self.customer_event_producer = CustomerEventProducer(self.connection, self.worker_id)
        self.customer_event_producer.initialize_rabbitmq()

        xprint("TicketWorker {}: initialize_rabbitmq() called".format(self.worker_id))

    def handle_ticket_event(self, ch, method, properties, body):
        ticket_event = TicketEvent(**json.loads(body))
        # To implement - This is the callback that is passed to "on_message_callback" when a message is received
        xprint("TicketWorker {}: handle_event() called".format(self.worker_id))
        # Handle the application logic and the publishing of events here

        customer_id = ticket_event.customer_id
        ride_number = ticket_event.ride_number
        purchase_time = ticket_event.purchase_time

        if customer_id in CUSTOMERS_DATABASE and ride_number in RIDES_DATABASE:
            # Append to ticket_events list
            self.ticket_events.append(ticket_event)

            # Create a CustomerEvent and publish it
            customer_event = CustomerEvent(customer_id=customer_id, ride_number=ride_number,
                                           cost=RIDES_DATABASE.get(ride_number), purchase_time=purchase_time)
            self.customer_event_producer.publish_customer_event(customer_event)
        else:
            # Negative acknowledgement for unknown customers or rides
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        # To implement - Start consuming from Rabbit
        self.channel.basic_consume(queue=self.queue, on_message_callback=self.handle_ticket_event, auto_ack=False)
        xprint("TicketWorker {}: start_consuming() called".format(self.worker_id))
        self.channel.start_consuming()

    def close(self):
        # Do not edit this method
        try:
            xprint("Closing worker with id = {}".format(self.worker_id))
            self.channel.stop_consuming()
            time.sleep(1)
            self.channel.close()
            self.customer_event_producer.close()
            time.sleep(1)
            self.connection.close()
        except Exception as e:
            print("Exception {} when closing worker with id = {}".format(
                e, self.worker_id))


class CustomerEventProducer:

    def __init__(self, connection, worker_id):
        # Do not edit the init method.
        self.worker_id = worker_id
        # Reusing connection created in TicketWorker
        self.channel = connection.channel()
        self.customer_event_exchange = "customer_events_exchange"

    def initialize_rabbitmq(self):
        # To implement - Initialize the RabbitMq connection, channel, exchange and queue here
        self.channel.exchange_declare(exchange=self.customer_event_exchange, exchange_type='topic')
        xprint("CustomerEventProducer {}: initialize_rabbitmq() called".format(self.worker_id))

    def publish_customer_event(self, customer_event):
        xprint("{}: CustomerEventProducer: Publishing customer event {}"
               .format(self.worker_id, vars(customer_event)))
        # To implement - publish a message to the Rabbitmq here
        # Use json.dumps(vars(customer_event)) to convert the ticket_event object to JSON
        customer_event_json = json.dumps(vars(customer_event))
        routing_key = f"customer.{customer_event.customer_id}.ride.{customer_event.ride_number}"
        self.channel.basic_publish(exchange=self.customer_event_exchange, routing_key=routing_key,
                                   body=customer_event_json)

    def close(self):
        # Do not edit this method
        self.channel.close()
