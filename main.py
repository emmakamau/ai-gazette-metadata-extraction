import argparse

from read_docs import AzureReadService
from dotenv import load_dotenv
import os

load_dotenv()

cognitive_services_key = os.getenv('COGNITIVE_SERVICES_KEY')
cognitive_services_endpoint = os.getenv('COGNITIVE_SERVICES_ENDPOINT')


def main():
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Analyze a PDF document using Azure Read Service.')
    subparsers = parser.add_subparsers(dest='command')

    # Create a subparser for the read_document function
    read_parser = subparsers.add_parser('read', help='Read a PDF document and extract text')
    read_parser.add_argument('file_path', type=str, help='Path to the PDF file to be analyzed')

    # Create a subparser for another function (e.g., extract_sections)
    extract_parser = subparsers.add_parser('extract', help='Extract sections from an analyzed document')
    extract_parser.add_argument('doc_id', type=str,
                                help='The JSON result from the analysis to extract sections from')

    csv_parser = subparsers.add_parser('csv', help='Export sections to a CSV file')
    csv_parser.add_argument('doc_ids', type=str, nargs='+', help='List of document IDs to be exported to CSV')

    args = parser.parse_args()

    azure_read_service = AzureReadService(cognitive_services_key, cognitive_services_endpoint)

    if args.command == 'read':
        azure_read_service.read_document(args.file_path)
    elif args.command == 'extract':
        azure_read_service.extract_sections(args.doc_id)
    elif args.command == "csv":
        azure_read_service.export_sections_to_csv(args.doc_ids)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
