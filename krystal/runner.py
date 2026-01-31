"""
Test Runner - Orchestrates testing across multiple services
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from krystal.config import ConfigManager, ServiceConfig, KrystalConfig
from krystal.crew.krystal_crew import KrystalCrew


class TestRunner:
    """Orchestrates end-to-end testing for multiple services"""

    def __init__(self, environment: str, service_names: Optional[List[str]] = None):
        """
        Initialize the test runner

        Args:
            environment: Environment name (dev, staging, prod)
            service_names: List of service names to test (None = all enabled services)
        """
        self.environment = environment
        self.service_names = service_names
        self.config_manager = ConfigManager(environment)
        self.results: List[Dict[str, Any]] = []
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

    def run(self) -> List[Dict[str, Any]]:
        """
        Run tests for all configured services (sequentially)

        Returns:
            List of test results for each service
        """
        self.start_time = datetime.now()
        self.results = []

        # Load configuration
        config = self.config_manager.load()

        # Get services to test
        if self.service_names:
            services = []
            for name in self.service_names:
                service = self.config_manager.get_service(name)
                if service:
                    services.append(service)
                else:
                    print(f"⚠️  Warning: Service '{name}' not found in configuration")
        else:
            services = self.config_manager.get_enabled_services()

        if not services:
            print("⚠️  No services to test")
            return []

        print(f"\n{'#' * 70}")
        print(f"# Krystal Test Execution")
        print(f"# Environment: {self.environment}")
        print(f"# Services: {len(services)}")
        print(f"# Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#' * 70}\n")

        # Run tests sequentially for each service
        for i, service in enumerate(services, 1):
            print(f"\n[{i}/{len(services)}] Testing service: {service.name}")
            print("-" * 60)

            try:
                # Create crew for this service
                krystal = KrystalCrew(
                    service_config=service,
                    sftp_config=config.sftp,
                    environment=self.environment,
                )

                # Run the test
                result = krystal.run()
                self.results.append(result)

            except Exception as e:
                print(f"❌ Error testing service {service.name}: {str(e)}")
                self.results.append(
                    {
                        "service": service.name,
                        "success": False,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                    }
                )

        self.end_time = datetime.now()

        # Print summary
        self._print_summary()

        return self.results

    def _print_summary(self):
        """Print execution summary"""
        duration = (self.end_time - self.start_time).total_seconds()
        passed = sum(1 for r in self.results if r.get("success", False))
        failed = len(self.results) - passed

        print(f"\n{'#' * 70}")
        print(f"# Test Execution Summary")
        print(f"{'#' * 70}")
        print(f"# Total Services: {len(self.results)}")
        print(f"# Passed: {passed} ✅")
        print(f"# Failed: {failed} ❌")
        print(f"# Duration: {duration:.2f} seconds")
        print(f"# End Time: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#' * 70}\n")

        # Print details
        for result in self.results:
            service = result.get("service", "Unknown")
            success = result.get("success", False)
            status = "✅ PASSED" if success else "❌ FAILED"

            if "error" in result:
                print(f"{service}: {status} - {result['error']}")
            else:
                print(f"{service}: {status}")

    def get_summary(self) -> Dict[str, Any]:
        """Get execution summary as dictionary"""
        if not self.start_time or not self.end_time:
            return {}

        duration = (self.end_time - self.start_time).total_seconds()
        passed = sum(1 for r in self.results if r.get("success", False))
        failed = len(self.results) - passed

        return {
            "environment": self.environment,
            "total_services": len(self.results),
            "passed": passed,
            "failed": failed,
            "duration_seconds": duration,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "results": self.results,
        }
