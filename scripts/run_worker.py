import sys
sys.path.append('../')

import argparse
from worker import TicketWorker

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run ticket worker")
    parser.add_argument('--id', '-i', type=str,
                        help="Worker ID", required=True)
    args = parser.parse_args()

    ticket_worker = TicketWorker(args.id)

    ticket_worker.initialize_rabbitmq()
    print(' [*] Worker waiting for TicketEvents. To exit press CTRL+C')
    ticket_worker.start_consuming()
