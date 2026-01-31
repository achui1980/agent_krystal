#!/usr/bin/env python3
"""
Krystal Test Runner

Main entry point for running end-to-end tests.
"""

import sys
import argparse
from pathlib import Path
from typing import List, Optional

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from krystal.runner import TestRunner
from krystal.report import ReportGenerator
from krystal.config import ConfigManager


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Krystal - End-to-End Testing Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all enabled services in dev environment
  python run_tests.py --env dev
  
  # Run specific services in staging
  python run_tests.py --env staging --services payment-service,invoice-service
  
  # Run with report generation
  python run_tests.py --env prod --report
  
  # Validate configuration
  python run_tests.py --env dev --validate-only
        """,
    )

    parser.add_argument(
        "--env",
        "-e",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment to run tests in (default: dev)",
    )

    parser.add_argument(
        "--services",
        "-s",
        help="Comma-separated list of service names to test (default: all enabled)",
    )

    parser.add_argument(
        "--report",
        "-r",
        action="store_true",
        help="Generate Markdown report after testing",
    )

    parser.add_argument("--report-path", help="Custom report output path")

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration without running tests",
    )

    parser.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List all configured services and exit",
    )

    return parser.parse_args()


def list_services(env: str):
    """List all configured services"""
    config_manager = ConfigManager(env)
    config = config_manager.load()

    print(f"\n📋 Services in '{env}' environment:\n")

    enabled = [s for s in config.services if s.enabled]
    disabled = [s for s in config.services if not s.enabled]

    if enabled:
        print("✅ Enabled Services:")
        for service in enabled:
            print(f"  • {service.name}: {service.description}")

    if disabled:
        print("\n⛔ Disabled Services:")
        for service in disabled:
            print(f"  • {service.name}: {service.description}")

    if not config.services:
        print("⚠️  No services configured")

    print()


def validate_config(env: str) -> bool:
    """Validate configuration"""
    import os

    print(f"\n🔍 Validating configuration for '{env}' environment...\n")

    try:
        config_manager = ConfigManager(env)
        config = config_manager.load()

        total = len(config.services)
        enabled_count = len([s for s in config.services if s.enabled])

        print(f"✅ Configuration loaded successfully")
        print(f"   • Total services: {total}")
        print(f"   • Enabled services: {enabled_count}")
        print(f"   • SFTP host: {config.sftp.host}")
        print(f"   • Report output: {config.report.output_path}")

        # Check required environment variables
        required_vars = ["OPENAI_API_KEY"]
        missing = [var for var in required_vars if not os.getenv(var)]

        if missing:
            print(f"\n⚠️  Missing environment variables: {', '.join(missing)}")
            return False
        else:
            print(f"\n✅ All required environment variables are set")

        print()
        return True

    except Exception as e:
        print(f"\n❌ Configuration error: {str(e)}\n")
        return False


def main():
    """Main entry point"""
    args = parse_args()

    # List services and exit
    if args.list:
        list_services(args.env)
        return 0

    # Validate configuration
    if args.validate_only:
        if validate_config(args.env):
            return 0
        return 1

    # Validate before running
    if not validate_config(args.env):
        return 1

    # Parse service names
    service_list: Optional[List[str]] = None
    if args.services:
        service_list = [s.strip() for s in args.services.split(",")]

    # Run tests
    runner = TestRunner(environment=args.env, service_names=service_list)
    results = runner.run()

    # Generate report if requested
    if args.report or args.report_path:
        config_manager = ConfigManager(args.env)
        config = config_manager.load()
        report_generator = ReportGenerator(config.report)

        report_file = report_generator.generate(results, args.report_path)
        print(f"\n📄 Report saved to: {report_file}")

    # Check results
    failed = sum(1 for r in results if not r.get("success", False))

    if failed > 0:
        print(f"\n❌ {failed} test(s) failed")
        return 1
    else:
        print("\n✅ All tests passed!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
