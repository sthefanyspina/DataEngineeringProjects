from soda.scan import Scan
import os

def run_quality_checks():
    scan = Scan()

    # Name from configuration.yml
    scan.set_data_source_name("csv_source")

    # Load configuration and checks
    scan.add_configuration_yaml_file("configuration.yml")
    scan.add_sodacl_yaml_file("checks.yml")

    print("🔍 Running data quality checks...\n")
    
    scan.execute()

    # Full log
    log_output = scan.get_logs_text()
    print("📄 Scan Report:\n")
    print(log_output)

    # Save report
    report_path = "../reports/scan_report.txt"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    with open(report_path, "w") as report_file:
        report_file.write(log_output)

    # Summary
    if scan.assert_no_error_nor_warning_logs():
        print("\n❌ Failures detected! Check reports/scan_report.txt for details.")
    else:
        print("\n✅ All checks passed successfully!")

    return scan

if __name__ == "__main__":
    run_quality_checks()
