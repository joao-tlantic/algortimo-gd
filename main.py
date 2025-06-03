#!/usr/bin/env python3
"""Main entry point for the my_new_project project."""

import click
import logging
import sys
import os
import subprocess
import winreg
from pathlib import Path
from datetime import datetime

# Import base_data_project components
from base_data_project.log_config import setup_logger
from base_data_project.utils import create_components
from base_data_project.process_management.manager import ProcessManager


# Define these things to ensure it finds the R installation
os.environ['R_HOME'] = 'C:\\Users\\antonio.alves\\AppData\\Local\\Programs\\R\\R-4.3.1' # TODO: get a better way of definning this
# Also try the user's AppData location if the default doesn't exist
if not os.path.exists(os.environ['R_HOME']):
    username = os.getenv('USERNAME')
    appdata_path = rf'C:\Users\{username}\AppData\Local\Programs\R\R-4.3.1'
    if os.path.exists(appdata_path):
        os.environ['R_HOME'] = appdata_path

# Add the R bin directory to PATH
r_home = os.environ['R_HOME']
r_bin = os.path.join(r_home, 'bin', 'x64')
if not os.path.exists(r_bin):
    r_bin = os.path.join(r_home, 'bin')

if r_bin not in os.environ['PATH']:
    os.environ['PATH'] = r_bin + os.pathsep + os.environ['PATH']

# Import project-specific components
from src.config import CONFIG, PROJECT_NAME
from src.services.example_service import AlgoritmoGDService

# Set up logger
logger = setup_logger(PROJECT_NAME, log_level=logging.INFO)

# Only run this if R_HOME is not already set
def find_r_home():
    """Find the R installation directory and set R_HOME environment variable."""
    # Skip if R_HOME is already set
    if os.environ.get('R_HOME'):
        print(f"R_HOME already set to: {os.environ['R_HOME']}")
        return True

    # Method 1: Try using the registry (most reliable on Windows)
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 'SOFTWARE\\R-core\\R') as key:
            r_home = winreg.QueryValueEx(key, 'InstallPath')[0]
            if os.path.exists(r_home):
                os.environ['R_HOME'] = r_home
                print(f"Found R in Windows registry: {r_home}")
                return True
    except Exception:
        pass

    # Method 2: Try registry with different location (sometimes used by different R installers)
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 'SOFTWARE\\R-Core\\R32\\3.0') as key:
            r_home = winreg.QueryValueEx(key, 'InstallPath')[0]
            if os.path.exists(r_home):
                os.environ['R_HOME'] = r_home
                print(f"Found R in Windows registry (alternate location): {r_home}")
                return True
    except Exception:
        pass

    # Method 3: Try using R RHOME command if R is in PATH
    try:
        r_path = subprocess.check_output(['R', '--vanilla', '--slave', '-e', 'cat(R.home())'], 
                                        text=True, stderr=subprocess.DEVNULL)
        if r_path and os.path.exists(r_path):
            os.environ['R_HOME'] = r_path
            print(f"Found R using R command: {r_path}")
            return True
    except Exception:
        pass

    # Method 4: Systematically search for R in common installation directories
    search_paths = []
    
    # Check Program Files
    program_files_paths = [
        'C:\\Program Files\\R',
        'C:\\Program Files (x86)\\R'
    ]
    
    # Add AppData paths for all users
    appdata_base = 'C:\\Users'
    if os.path.exists(appdata_base):
        for user in os.listdir(appdata_base):
            user_path = os.path.join(appdata_base, user)
            # Skip files and system directories
            if not os.path.isdir(user_path) or user in ['Public', 'Default', 'Default User', 'All Users']:
                continue
            appdata_path = os.path.join(user_path, 'AppData', 'Local', 'Programs', 'R')
            if os.path.exists(appdata_path):
                search_paths.append(appdata_path)

    # Add manually specified path known to work on your system
    search_paths.append('C:\\Users\\antonio.alves\\AppData\\Local\\Programs\\R\\R-4.3.1')
    
    # Search program files directories first
    for pf in program_files_paths:
        if os.path.exists(pf):
            # Look for R version folders (like R-4.3.1)
            search_paths.extend([
                os.path.join(pf, d) for d in os.listdir(pf) 
                if os.path.isdir(os.path.join(pf, d)) and d.startswith('R-')
            ])
    
    # Add specific common R versions as fallback
    for version in ['4.3.1', '4.3.0', '4.2.3', '4.2.2', '4.2.1', '4.2.0', '4.1.3', '4.1.2', '4.1.1', '4.1.0']:
        search_paths.append(f'C:\\Program Files\\R\\R-{version}')

    # Search all paths
    for path in search_paths:
        if not os.path.exists(path):
            continue
            
        # If this is a directory containing R version directories, check each version
        if any(d.startswith('R-') for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))):
            # Find the newest R version directory
            r_dirs = [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d)) and d.startswith('R-')]
            if r_dirs:
                # Sort by version (assuming R-x.y.z format)
                r_dirs.sort(key=lambda x: [int(n) if n.isdigit() else n for n in x.replace('R-', '').split('.')])
                newest_r = os.path.join(path, r_dirs[-1])  # Get the latest version
                
                if os.path.exists(os.path.join(newest_r, 'bin')):
                    os.environ['R_HOME'] = newest_r
                    print(f"Found R installation: {newest_r}")
                    return True
        # Direct R installation directory
        elif os.path.exists(os.path.join(path, 'bin')):
            os.environ['R_HOME'] = path
            print(f"Found R installation: {path}")
            return True

    # If we got here, we couldn't find R
    print("\nWARNING: Could not find R installation.")
    print("Please make sure R is installed on this system.")
    print("This template requires R to function correctly.")
    print("R can be downloaded from: https://cran.r-project.org/bin/windows/base/\n")
    return False

# Find and set R_HOME before any imports that might need it
# if find_r_home():
#     # Also set PATH to include R bin directory for DLL loading
#     r_home = os.environ['R_HOME']
#     r_bin = os.path.join(r_home, 'bin', 'x64') if os.path.exists(os.path.join(r_home, 'bin', 'x64')) else os.path.join(r_home, 'bin')
#     
#     # Add R_bin to PATH if not already there
#     if r_bin not in os.environ['PATH']:
#         os.environ['PATH'] = r_bin + os.pathsep + os.environ['PATH']
#         print(f"Added R bin directory to PATH: {r_bin}")
#     
#     # Set additional R environment variables that might be needed
#     if not os.environ.get('R_USER'):
#         os.environ['R_USER'] = os.path.expanduser('~')
#     
#     # Set R_LIBS_USER if not set
#     if not os.environ.get('R_LIBS_USER'):
#         os.environ['R_LIBS_USER'] = os.path.join(os.environ['R_USER'], 'R', 'win-library')

@click.group()
def cli():
    """Interactive command-line interface for the my_new_project project."""
    pass

# Rest of your code remains the same
@cli.command(help="Run the interactive process")
@click.option("--use-db/--use-csv", prompt="Use database for data storage", default=False, 
              help="Use database instead of CSV files")
@click.option("--no-tracking/--enable-tracking", default=False, 
              help='Disable process tracking (reduces overhead)')
def run_process(use_db, no_tracking):
    """
    Interactive process run with enhanced user experience
    """
    # Display header
    click.clear()
    click.echo(click.style(f"=== {PROJECT_NAME} Interactive Mode ===", fg="green", bold=True))
    click.echo(click.style(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", fg="green"))
    click.echo()
    
    # Display configuration
    click.echo(click.style("Configuration:", fg="blue"))
    click.echo(f"Data source: {'Database' if use_db else 'CSV files'}")
    click.echo(f"Process tracking: {'Disabled' if no_tracking else 'Enabled'}")
    click.echo()
    
    try:
        logger.info("Starting the Interactive Process")
        click.echo("Initializing components...")
        
        # Create spinner for initialization
        with click.progressbar(length=100, label="Initializing") as bar:
            # Create and configure components
            data_manager, process_manager = create_components(use_db=use_db, no_tracking=no_tracking, config=CONFIG)

            # TODO: Remove this
            # Debug: Check what's in the process_manager
            # Right after: data_manager, process_manager = create_components(...)
            print("=== DEBUG PROCESS MANAGER ===")
            if process_manager:
                print(f"ProcessManager type: {type(process_manager)}")
                print(f"ProcessManager attributes: {[attr for attr in dir(process_manager) if not attr.startswith('_')]}")
                print(f"Has config: {hasattr(process_manager, 'config')}")
                print(f"Has core_data: {hasattr(process_manager, 'core_data')}")
                if hasattr(process_manager, 'core_data'):
                    print(f"Core data type: {type(process_manager.core_data)}")
                    if isinstance(process_manager.core_data, dict):
                        print(f"Core data keys: {list(process_manager.core_data.keys())}")
            else:
                print("No process_manager created")
            print("=== END DEBUG ===")
        
            bar.update(100)

        click.echo()
        click.echo(click.style("Components initialized successfully", fg="green"))
        click.echo()
        
        with data_manager:
            external_call_dict = CONFIG.get('external_call_data', {})
            print(external_call_dict)
            # Create service with data and process managers
            service = AlgoritmoGDService(
                data_manager=data_manager,
                process_manager=process_manager,
                external_call_dict=external_call_dict,
                config=CONFIG
            )
            
            # Initialize process
            process_id = service.initialize_process(
                "Interactive Process Run", 
                f"Process run on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            
            # Run each stage with user interaction
            stages = ['data_loading', 'processing', 'result_analysis']
            for stage in stages:
                click.echo(click.style(f"Stage: {stage}", fg="blue", bold=True))
                
                # Let user decide whether to run this stage
                if click.confirm(f"Execute {stage} stage?", default=True):
                    success = service.execute_stage(stage)
                    
                    #TODO: remove this
                    #click.echo(f"valid_emp_df: {service.data.auxiliary_data['valid_emp']}")

                    if success:
                        click.echo(click.style(f"✓ {stage} completed successfully", fg="green"))
                    else:
                        click.echo(click.style(f"✘ {stage} failed", fg="red", bold=True))
                        if not click.confirm("Continue despite failure?", default=False):
                            click.echo("Process terminated by user")
                            break
                else:
                    click.echo(f"Skipping {stage} stage")
                
                click.echo()
            
            # Process complete
            click.echo(click.style("Process complete", fg="green", bold=True))
            
            # Get and display process summary
            if process_manager:
                process_summary = service.get_process_summary()
                status_counts = process_summary.get('status_counts', {})
                
                click.echo(click.style("Process Summary:", fg="blue", bold=True))
                click.echo(f"Process ID: {process_id}")
                click.echo(f"Completed stages: {status_counts.get('completed', 0)}")
                click.echo(f"Failed stages: {status_counts.get('failed', 0)}")
                click.echo(f"Skipped stages: {status_counts.get('skipped', 0)}")
                click.echo(f"Overall progress: {process_summary.get('progress', 0) * 100:.1f}%")
                
    except Exception as e:
        logger.error(f"Process failed: {str(e)}", exc_info=True)
        click.echo(click.style(f"✘ Process failed: {str(e)}", fg="red", bold=True))
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(cli())  # Return exit code