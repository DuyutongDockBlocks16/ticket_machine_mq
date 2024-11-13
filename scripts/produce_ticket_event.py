import sys
sys.path.append('../')

import argparse
from datetime import datetime, timezone, timedelta
from db_and_event_definitions import TicketEvent, CUSTOMERS_DATABASE, RIDES_DATABASE
from ticket_machine import TicketEventProducer
from worker import *

def get_datetime_now():
    tz_offset = +2.0  # EET Timezone
    tzinfo = timezone(timedelta(hours=tz_offset))
    dt = datetime.now(tzinfo)
    return dt.isoformat()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Send ticket event message")
    parser.add_argument('--customer_id', '-c', type=str,
                        choices=CUSTOMERS_DATABASE,
                        help="Customer ID")
    parser.add_argument('--ride_number', '-r', type=str,
                        choices=RIDES_DATABASE.keys(),
                        help="Ride Number")
    args = parser.parse_args()

    ticket_event_producer = TicketEventProducer()
    ticket_event_producer.initialize_rabbitmq()
    ticket_event = TicketEvent(
        args.customer_id, args.ride_number, get_datetime_now())
    ticket_event_producer.publish_ticket_event(ticket_event)
    ticket_event_producer.close()
