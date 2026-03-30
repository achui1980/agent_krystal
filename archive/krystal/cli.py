"""
Krystal CLI - Command Line Interface
"""

import os
import click
from typing import List, Optional
from dotenv import load_dotenv

from krystal.runner import TestRunner
from krystal.report import ReportGenerator
from krystal.config import ConfigManager

# Load environment variables
load_dotenv()


@click.group()
@click.version_option(version="1.0.0", prog_name="krystal")
def cli():
    """Krystal - End-to-End Testing Agent powered by CrewAI"""
    pass


@cli.command()
@click.option("--env", "-e", default="dev", help="Environment (dev, staging, prod)")
@click.option(
    "--services",
    "-s",
    help="Comma-separated list of service names (default: all enabled)",
)
@click.option("--report", "-r", is_flag=True, help="Generate report after testing")
@click.option("--report-path", help="Custom report output path")
def run(env: str, services: Optional[str], report: bool, report_path: Optional[str]):
    """Run end-to-end tests for specified services"""

    # Parse service names
    service_list: Optional[List[str]] = None
    if services:
        service_list = [s.strip() for s in services.split(",")]

    # Create and run test runner
    runner = TestRunner(environment=env, service_names=service_list)
    results = runner.run()

    # Generate report if requested
    if report or report_path:
        config_manager = ConfigManager(env)
        config = config_manager.load()
        report_generator = ReportGenerator(config.report)

        output_path = report_path or None
        report_file = report_generator.generate(results, output_path)

        click.echo(f"\n📄 Report saved to: {report_file}")

    # Exit with error code if any test failed
    failed = sum(1 for r in results if not r.get("success", False))
    if failed > 0:
        click.echo(f"\n❌ {failed} test(s) failed")
        raise click.Exit(code=1)
    else:
        click.echo("\n✅ All tests passed!")


@cli.command()
@click.option("--env", "-e", default="dev", help="Environment (dev, staging, prod)")
def list(env: str):
    """List all configured services"""

    config_manager = ConfigManager(env)
    config = config_manager.load()

    click.echo(f"\n📋 Services in '{env}' environment:\n")

    enabled_services = []
    disabled_services = []

    for service in config.services:
        if service.enabled:
            enabled_services.append(service)
        else:
            disabled_services.append(service)

    if enabled_services:
        click.echo("✅ Enabled Services:")
        for service in enabled_services:
            click.echo(f"  • {service.name}: {service.description}")

    if disabled_services:
        click.echo("\n⛔ Disabled Services:")
        for service in disabled_services:
            click.echo(f"  • {service.name}: {service.description}")

    if not config.services:
        click.echo("⚠️  No services configured")

    click.echo()


@cli.command()
@click.option("--env", "-e", default="dev", help="Environment (dev, staging, prod)")
def validate(env: str):
    """Validate configuration"""

    click.echo(f"\n🔍 Validating configuration for '{env}' environment...\n")

    try:
        config_manager = ConfigManager(env)
        config = config_manager.load()

        # Check services
        total = len(config.services)
        enabled = len([s for s in config.services if s.enabled])

        click.echo(f"✅ Configuration loaded successfully")
        click.echo(f"   • Total services: {total}")
        click.echo(f"   • Enabled services: {enabled}")
        click.echo(f"   • SFTP host: {config.sftp.host}")
        click.echo(f"   • Report output: {config.report.output_path}")

        # Check for required environment variables
        required_vars = ["OPENAI_API_KEY"]
        missing = []

        for var in required_vars:
            if not os.getenv(var):
                missing.append(var)

        if missing:
            click.echo(f"\n⚠️  Missing environment variables: {', '.join(missing)}")
            raise click.Exit(code=1)
        else:
            click.echo(f"\n✅ All required environment variables are set")

        click.echo()

    except Exception as e:
        click.echo(f"\n❌ Configuration error: {str(e)}")
        raise click.Exit(code=1)


def main():
    """Entry point for CLI"""
    cli()


if __name__ == "__main__":
    main()
