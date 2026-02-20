"""
Main entry point for Krystal V4 CLI.
"""

import argparse
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from crewai import LLM
from krystal_v4.crew import KrystalV4Crew
from krystal_v4.utils.logger import logger
from krystal_v4.utils.error_handler import handle_error_gracefully, UserAbortError


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Krystal V4 - ETL Test Data Generator powered by CrewAI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate test data for humanaS10 case (using default gpt-4o-mini)
  python -m krystal_v4.main --rules case/rules.csv --case humanaS10 --count 10

  # Use GPT-4o for better accuracy (higher cost)
  python -m krystal_v4.main --rules case/rules.csv --case humanaS10 --count 10 --model gpt-4o

  # Increase creativity with temperature
  python -m krystal_v4.main --rules case/rules.csv --case humanaS10 --count 10 --temperature 0.3

  # Custom case directory with all options
  python -m krystal_v4.main --rules case/rules.csv --case highmark --case-dir case/highmark --count 20 --model gpt-4o --temperature 0.1
        """,
    )

    parser.add_argument(
        "--rules", required=True, help="Path to rules.csv file (required)"
    )

    parser.add_argument(
        "--case", required=True, help="Test case name/identifier (required)"
    )

    parser.add_argument(
        "--case-dir",
        default=None,
        help="Directory containing reference files (default: case/{case_name})",
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of records to generate (default: 10)",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    # LLM Configuration
    parser.add_argument(
        "--model",
        default="gpt-5.2",
        help="OpenAI model to use (default: gpt-4o-mini, options: gpt-4o, gpt-4-turbo, gpt-3.5-turbo)",
    )

    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="LLM temperature for creativity (0.0-2.0, default: 0.0 for deterministic output)",
    )

    parser.add_argument(
        "--max-tokens",
        type=int,
        default=131072,
        help="Maximum tokens for LLM response (default: 4096)",
    )

    return parser.parse_args()


def validate_inputs(args):
    """Validate input arguments."""
    errors = []

    # Check rules file exists
    if not os.path.isfile(args.rules):
        errors.append(f"Rules file not found: {args.rules}")

    # Check case directory exists (if specified)
    if args.case_dir and not os.path.isdir(args.case_dir):
        errors.append(f"Case directory not found: {args.case_dir}")

    # Check record count is positive
    if args.count <= 0:
        errors.append(f"Record count must be positive, got: {args.count}")

    # Validate temperature range
    if not 0.0 <= args.temperature <= 2.0:
        errors.append(
            f"Temperature must be between 0.0 and 2.0, got: {args.temperature}"
        )

    # Validate max_tokens
    if args.max_tokens < 100:
        errors.append(f"Max tokens must be at least 100, got: {args.max_tokens}")

    if errors:
        logger.error("Input validation failed:")
        for error in errors:
            logger.error(f"  - {error}")
        sys.exit(1)


def main():
    """Main entry point."""
    # Load environment variables
    load_dotenv()

    # Check for OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        logger.error("OPENAI_API_KEY not found in environment variables")
        logger.error("Please set OPENAI_API_KEY in .env file or environment")
        sys.exit(1)

    # Parse arguments
    args = parse_args()

    # Validate inputs
    validate_inputs(args)

    # Display banner
    logger.info("")
    logger.info("╔════════════════════════════════════════════════════════════════╗")
    logger.info("║                    Krystal V4                                   ║")
    logger.info("║            ETL Test Data Generator                              ║")
    logger.info("║                Powered by CrewAI                                ║")
    logger.info("╚════════════════════════════════════════════════════════════════╝")
    logger.info("")
    logger.info(f"🤖 LLM Configuration:")
    logger.info(f"   Model: {args.model}")
    logger.info(f"   Temperature: {args.temperature}")
    logger.info(f"   Max Tokens: {args.max_tokens}")
    logger.info("")

    try:
        # Initialize LLM with custom configuration using CrewAI's LLM class
        llm = LLM(model=f"openai/{args.model}", temperature=args.temperature)

        # Create and run crew
        crew = KrystalV4Crew(
            rules_file=args.rules,
            case_name=args.case,
            case_dir=args.case_dir,
            record_count=args.count,
            llm=llm,
        )

        result = crew.run()

        # Display success message
        logger.info("")
        logger.info("✓ Test data generation completed successfully!")
        logger.info("")
        logger.info("Output files:")
        logger.info(f"  📄 Config:   {result['files']['config']}")
        logger.info(f"  📄 Source:   {result['files']['source']}")
        logger.info(f"  📄 Expected: {result['files']['expected']}")
        logger.info("")

        return 0

    except UserAbortError as e:
        logger.error(f"Execution aborted: {e}")
        return 1

    except KeyboardInterrupt:
        logger.info("")
        logger.info("Execution interrupted by user (Ctrl+C)")
        return 130

    except Exception as e:
        handle_error_gracefully(e, context="Main execution")
        return 1


if __name__ == "__main__":
    sys.exit(main())
