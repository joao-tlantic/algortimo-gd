import pandas as pd
import math
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
import logging
from base_data_project.log_config import get_logger
from src.config import PROJECT_NAME

# Set up logger
logger = get_logger(PROJECT_NAME)

def read_data_alcampo(medium_dataframes: Dict[str, pd.DataFrame]) -> Tuple[Any, ...]:
    """
    Enhanced version of read_data_alcampo with comprehensive logging and error checks.
    
    Args:
        medium_dataframes: Dictionary containing the required DataFrames
        
    Returns:
        Tuple containing all processed data elements for the algorithm
        
    Raises:
        ValueError: If required DataFrames are missing or invalid
        KeyError: If required columns are missing from DataFrames
    """
    try:
        logger.info("Starting enhanced data reading for Alcampo algorithm")
        
        # =================================================================
        # 1. VALIDATE INPUT data
        # =================================================================
        required_dataframes = ['df_colaborador', 'df_estimativas', 'df_calendario']
        missing_dataframes = [df for df in required_dataframes if df not in medium_dataframes]
        
        if missing_dataframes:
            raise ValueError(f"Missing required DataFrames: {missing_dataframes}")
        
        # Extract DataFrames
        # matriz_colaborador_gd = pd.read_csv('src/algorithms/shift_scheduler/data/matriz_colaborador_alcampos3.csv', sep=';',  engine='python')
        # matriz_estimativas_gd = pd.read_csv('src/algorithms/shift_scheduler/data/matriz_estimativas_alcampos2.csv', index_col=0)
        # matriz_calendario_gd = pd.read_csv('src/algorithms/shift_scheduler/data/matriz_calendario_alcampos3.csv', index_col=0)

        matriz_colaborador_gd = medium_dataframes['df_colaborador'].copy()
        matriz_estimativas_gd = medium_dataframes['df_estimativas'].copy() 
        matriz_calendario_gd = medium_dataframes['df_calendario'].copy()

        matriz_colaborador_gd.columns = matriz_colaborador_gd.columns.str.lower()
        matriz_estimativas_gd.columns = matriz_estimativas_gd.columns.str.lower()
        matriz_calendario_gd.columns = matriz_calendario_gd.columns.str.lower()       

        logger.info(f"Input DataFrames loaded:")
        logger.info(f"  - matriz_colaborador: {matriz_colaborador_gd.shape}")
        logger.info(f"  - matriz_estimativas: {matriz_estimativas_gd.shape}")
        logger.info(f"  - matriz_calendario: {matriz_calendario_gd.shape}")
        
        # =================================================================
        # 2. VALIDATE REQUIRED COLUMNS
        # =================================================================
        required_colaborador_cols = ['matricula', 'L_TOTAL', 'L_DOM', 'C2D', 'C3D', 'L_D', 'CXX', 'VZ', 'L_RES', 'L_RES2']
        required_colaborador_cols = [s.lower() for s in required_colaborador_cols]
        required_calendario_cols = ['colaborador', 'data', 'wd', 'dia_tipo', 'tipo_turno']
        required_calendario_cols = [s.lower() for s in required_calendario_cols]
        
        missing_colab_cols = [col for col in required_colaborador_cols if col not in matriz_colaborador_gd.columns]
        missing_cal_cols = [col for col in required_calendario_cols if col not in matriz_calendario_gd.columns]
        
        if missing_colab_cols:
            raise KeyError(f"Missing required columns in matriz_colaborador: {missing_colab_cols}")
        if missing_cal_cols:
            raise KeyError(f"Missing required columns in matriz_calendario: {missing_cal_cols}")
        
        logger.info("[OK] All required columns present in DataFrames")
        
        matriz_calendario_gd = matriz_calendario_gd[matriz_calendario_gd["matricula"] != "TIPO_DIA"]
        matriz_colaborador_gd = matriz_colaborador_gd[matriz_colaborador_gd["matricula"] != "TIPO_DIA"]

        # =================================================================
        # 3. CALCULATE L_Q FOR colaborador data
        # =================================================================
        logger.info("Calculating L_Q values for workers")
        
        # Check for missing values in required columns
        numeric_cols = ['L_TOTAL', 'L_DOM', 'C2D', 'C3D', 'L_D', 'CXX', 'VZ', 'L_RES', 'L_RES2']
        numeric_cols = [s.lower() for s in numeric_cols]


        for col in numeric_cols:
            if matriz_colaborador_gd[col].isna().any():
                logger.warning(f"Found NaN values in column {col}, filling with 0")
                matriz_colaborador_gd[col] = matriz_colaborador_gd[col].fillna(0)
        
        matriz_colaborador_gd["l_q"] = (
            matriz_colaborador_gd["l_total"] - 
            matriz_colaborador_gd["l_dom"] - 
            matriz_colaborador_gd["c2d"] - 
            matriz_colaborador_gd["c3d"] - 
            matriz_colaborador_gd["l_d"] - 
            matriz_colaborador_gd["cxx"] - 
            matriz_colaborador_gd["vz"] - 
            matriz_colaborador_gd["l_res"] - 
            matriz_colaborador_gd["l_res2"]
        )
        
        logger.info(f"L_Q calculated. Range: {matriz_colaborador_gd['l_q'].min():.2f} to {matriz_colaborador_gd['l_q'].max():.2f}")
        
        # =================================================================
        # 4. PROCESS CALENDARIO data
        # =================================================================
        logger.info("Processing calendario data")
        
        # Ensure colaborador column is numeric
        matriz_calendario_gd['colaborador'] = pd.to_numeric(matriz_calendario_gd['colaborador'], errors='coerce')
        invalid_colaborador = matriz_calendario_gd['colaborador'].isna().sum()
        if invalid_colaborador > 0:
            logger.warning(f"Found {invalid_colaborador} invalid colaborador values, removing these rows")
            matriz_calendario_gd = matriz_calendario_gd.dropna(subset=['colaborador'])
        
        # Convert data column to datetime
        try:
            matriz_calendario_gd['data'] = pd.to_datetime(matriz_calendario_gd['data'])
            matriz_estimativas_gd['data'] = pd.to_datetime(matriz_estimativas_gd['data'])
            logger.info(f"Date range: {matriz_calendario_gd['data'].min()} to {matriz_calendario_gd['data'].max()}")
        except Exception as e:
            raise ValueError(f"Error converting data column to datetime: {e}")
        

        # =================================================================
        # 5. IDENTIFY VALID WORKERS (PRESENT IN ALL DATAFRAMES)
        # =================================================================
        logger.info("Identifying valid workers present in all DataFrames")
        
        # Get unique workers from each DataFrame
        workers_colaborador = set(matriz_colaborador_gd['matricula'].dropna().astype(int))
        workers_calendario = set(matriz_calendario_gd['colaborador'].dropna().astype(int))
        
        
        logger.info(f"Workers found:")
        logger.info(f"  - In matriz_colaborador: {len(workers_colaborador)} workers")
        logger.info(f"  - In matriz_calendario: {len(workers_calendario)} workers")


        valid_workers = workers_colaborador.intersection(workers_calendario)
        
        if not valid_workers:
            raise ValueError("No workers found that are present in all required DataFrames")
        
        workers = sorted(list(valid_workers))
        logger.info(f"[OK] Final valid workers: {len(workers)} workers")
        logger.info(f"   Worker IDs: {workers[:10]}{'...' if len(workers) > 10 else ''}")
        

        # Ensure data type consistency before filtering
        matriz_colaborador_gd['matricula'] = matriz_colaborador_gd['matricula'].astype(int)
        matriz_calendario_gd['colaborador'] = matriz_calendario_gd['colaborador'].astype(int)
        
        matriz_colaborador_gd = matriz_colaborador_gd[matriz_colaborador_gd['matricula'].isin(workers)]
        matriz_calendario_gd = matriz_calendario_gd[matriz_calendario_gd['colaborador'].isin(workers)]

        
        logger.info(f"Filtered DataFrames to valid workers:")
        logger.info(f"  - matriz_colaborador: {matriz_colaborador_gd.shape}")
        logger.info(f"  - matriz_estimativas: {matriz_estimativas_gd.shape}")
        logger.info(f"  - matriz_calendario: {matriz_calendario_gd.shape}")
        
        # =================================================================
        # 6. EXTRACT DAYS AND DATE INFORMATION
        # =================================================================
        logger.info("Extracting days and date information")
        
        days_of_year = sorted(matriz_calendario_gd['data'].dt.dayofyear.unique().tolist())
        logger.info(f"Days of year: {len(days_of_year)} days (from {min(days_of_year)} to {max(days_of_year)})")
        
        # =================================================================
        # 7. IDENTIFY SPECIAL DAYS
        # =================================================================
        logger.info("Identifying special days")
        
        # Define shifts and special days
        shifts = ["M", "T", "L","LQ", "F", "V","LD", "A"]
        
        sundays = sorted(matriz_calendario_gd[matriz_calendario_gd['wd'] == 'Sun']['data'].dt.dayofyear.unique().tolist())

        holidays = sorted(matriz_calendario_gd[
            (matriz_calendario_gd['wd'] != 'Sun') & 
            (matriz_calendario_gd["dia_tipo"] == "domYf")
        ]['data'].dt.dayofyear.unique().tolist())
        
        closed_holidays = sorted(matriz_calendario_gd[
            matriz_calendario_gd['tipo_turno'] == "F"
        ]['data'].dt.dayofyear.unique().tolist())
        
        special_days = sorted(list(set(sundays + holidays)))
        
        logger.info(f"Special days identified:")
        logger.info(f"  - Sundays: {len(sundays)} days")
        logger.info(f"  - Holidays (non-Sunday): {len(holidays)} days")
        logger.info(f"  - Closed holidays: {len(closed_holidays)} days")
        logger.info(f"  - Total special days: {len(special_days)} days")
        
        # =================================================================
        # 8. PROCESS WORKER-SPECIFIC data
        # =================================================================
        logger.info("Processing worker-specific data")
        
        # Initialize dictionaries for worker-specific information
        empty_days = {}
        worker_holiday = {}
        missing_days = {}
        last_registered_day = {}
        first_registered_day = {}
        working_days = {}
        
        # Process each worker
        for w in workers:
            worker_calendar = matriz_calendario_gd[matriz_calendario_gd['colaborador'] == w]
            
            if worker_calendar.empty:
                logger.warning(f"No calendar data found for worker {w}")
                empty_days[w] = []
                worker_holiday[w] = []
                missing_days[w] = []
                continue
            
            # Find days with specific statuses
            worker_empty = worker_calendar[worker_calendar['tipo_turno'] == '-']['data'].dt.dayofyear.tolist()
            worker_missing = worker_calendar[worker_calendar['tipo_turno'] == 'V']['data'].dt.dayofyear.tolist()
            w_holiday = worker_calendar[worker_calendar['tipo_turno'] == 'A']['data'].dt.dayofyear.tolist()

            empty_days[w] = worker_empty
            worker_holiday[w] = w_holiday
            missing_days[w] = worker_missing
            
        # Track first and last registered days
            if w in matriz_calendario_gd['colaborador'].values:
                first_registered_day[w] = worker_calendar['data'].dt.dayofyear.min()
            else:
                first_registered_day[w] = 0

            if w in matriz_calendario_gd['colaborador'].values:
                last_registered_day[w] = worker_calendar['data'].dt.dayofyear.max()
            else:
                last_registered_day[w] = 0

        

        for w in workers:
            # Mark all remaining days after last_registered_day as 'A' (absent)
            if first_registered_day[w] > 0 or last_registered_day[w] > 0:  # Ensure worker was registered at some point
                missing_days[w].extend([d for d in range( 1, first_registered_day[w]) if d not in missing_days[w]])
                missing_days[w].extend([d for d in range(last_registered_day[w] + 1, 366) if d not in missing_days[w]])
            
            empty_days[w] = list(set(empty_days[w]) - set(closed_holidays))
            worker_holiday[w] = list(set(worker_holiday[w]) - set(closed_holidays))
            missing_days[w] = list(set(missing_days[w]) - set(closed_holidays))

            working_days[w] = set(days_of_year) - set(empty_days[w]) - set(worker_holiday[w]) - set(missing_days[w]) - set(closed_holidays)

            if not working_days[w]:
                logger.warning(f"Worker {w} has no working days after processing. This may indicate an issue with the data.")

        
        logger.info(f"Worker-specific data processed for {len(workers)} workers")
        
        # =================================================================
        # 9. CALCULATE ADDITIONAL PARAMETERS
        # =================================================================
        logger.info("Calculating additional parameters")
        
        # Working days (non-special days)
        non_holidays = [d for d in days_of_year if d not in closed_holidays]  # Alias for compatibility
        
        # Calculate week information
        unique_dates = sorted(matriz_calendario_gd['data'].unique())

        if unique_dates:
            start_weekday = matriz_estimativas_gd["wday"].iloc[0]  # 1=Sunday, 7=Saturday
            
            # Create week to days mapping
            week_to_days = {}

            for i, day in enumerate(days_of_year):
                # Simple approach: calculate day of week, then determine week number
                day_of_week = (start_weekday - 3 + day) % 7 + 1  # 0=Mon, 6=Sun
                week_num = (start_weekday - 3 + day) // 7 + 1
                
                # Initialize the week list if it doesn't exist
                if week_num not in week_to_days:
                    week_to_days[week_num] = []
                
                # Add the day to its corresponding week
                week_to_days[week_num].append(day)
        else:
            start_weekday = 0
            week_to_days = {}
            logger.warning("No unique dates found in matriz_calendario_gd, week calculations may be incomplete")
        
        logger.info(f"Week calculation:")
        logger.info(f"  - Start weekday: {start_weekday}")
        logger.info(f"  - Number of weeks: {len(week_to_days)}")
        logger.info(f"  - Working days: {len(working_days)} days")
        
        # =================================================================
        # 10.1. EXTRACT WORKER CONTRACT INFORMATION
        # =================================================================
        logger.info("Extracting worker contract information")
        
        # Create dictionaries for worker contract data
        contract_type = {}
        total_l = {}
        total_l_dom = {}
        c2d = {}
        c3d = {}
        l_d = {}
        l_q = {}
        cxx = {}
        t_lq = {}
        tc = {}
        
        for w in workers:
            worker_data = matriz_colaborador_gd[matriz_colaborador_gd['matricula'] == w]
            
            if worker_data.empty:
                logger.warning(f"No contract data found for worker {w}")
                # Set default values
                contract_type[w] = 'Contract Error'  # Default contract type
                total_l[w] = 0
                total_l_dom[w] = 0
                c2d[w] = 0
                c3d[w] = 0
                l_d[w] = 0
                l_q[w] = 0
                cxx[w] = 0
                t_lq[w] = 0
                tc[w] = 0
            else:
                worker_row = worker_data.iloc[0]  # Take first row if multiple
                
                # Extract contract information
                contract_type[w] = worker_row.get('tipo_contrato', 'Contract Error')
                total_l[w] = worker_row.get('l_total', 0)
                total_l_dom[w] = worker_row.get('l_dom', 0)
                c2d[w] = worker_row.get('c2d', 0)
                c3d[w] = worker_row.get('c3d', 0)
                l_d[w] = worker_row.get('l_d', 0)
                l_q[w] = worker_row.get('l_q', 0)
                cxx[w] = worker_row.get('cxx', 0)
                t_lq[w] = worker_row.get('l_q', 0) + worker_row.get('c2d', 0) + worker_row.get('c3d', 0)
                tc[w] = worker_row.get('dofhc', 0)
        
        logger.info(f"Contract information extracted for {len(workers)} workers")

        # =================================================================
        # 10.2. ADAPT PROPORTIONS FOR WORKERS FOR FIRST AND LAST DAYS
        # =================================================================
        logger.info("Adjusting worker parameters based on last registered days")

        for w in workers:
            if (last_registered_day[w] > 0 and last_registered_day[w] < 365):
                proportion = last_registered_day[w]  / 365
                total_l[w] = round(proportion * total_l[w])
                total_l_dom[w] = round(proportion * total_l_dom[w])
                c2d[w] = math.floor(proportion * c2d[w])
                c3d[w] = math.floor(proportion * c3d[w])
                l_d[w] = round(proportion * l_d[w])
                l_q[w] = round(proportion * l_q[w])
                cxx[w] = round(proportion * cxx[w])
                t_lq[w] = round(proportion * t_lq[w])
                tc[w] = round(proportion * tc[w])

        for w in workers:
            worker_special_days = [d for d in special_days if d in working_days[w]]
            if contract_type[w] == 6:
                total_l_dom[w] = len(worker_special_days) - l_d[w] - tc[w]
            elif contract_type[w] in [4,5]:
                total_l_dom[w] = len(worker_special_days) - l_d[w] - tc[w]

        for w in workers:
            if contract_type[w] == 6:
                l_d[w] =  l_d[w] + tc[w] 
            elif contract_type[w] in [4,5]:
                total_l[w] = total_l[w] - tc[w]        

        logger.info("Worker parameters adjusted based on first and last registered days")

        # =================================================================
        # 11. PROCESS ESTIMATIVAS data
        # =================================================================
        logger.info("Processing estimativas data")
        
        # Extract optimization parameters from estimativas
        pess_obj = {}
        min_workers = {}
        max_workers = {}
        working_shift = ["M", "T", "TC"]

        # If estimativas has specific data, process it
        if not matriz_estimativas_gd.empty:
            
            for d in days_of_year:
                
                # Process pess_obj for working_shift
                for s in working_shift:

                    day_shift_data = matriz_estimativas_gd[(matriz_estimativas_gd['data'].dt.dayofyear == d) & (matriz_estimativas_gd['turno'] == s)]
                    if not day_shift_data.empty:
                        # Convert float to integer for OR-Tools compatibility
                        pess_obj[(d, s)] = int(round(day_shift_data['pess_obj'].values[0]))
                    else:
                        pess_obj[(d, s)] = 0  # or any default value you prefer
                
                # Process min/max workers for all shifts
                for shift_type in shifts:
                    day_shift_data = matriz_estimativas_gd[(matriz_estimativas_gd['data'].dt.dayofyear == d) & (matriz_estimativas_gd['turno'] == shift_type)]
                    if not day_shift_data.empty:
                                    # Convert floats to integers for OR-Tools compatibility
                                    min_workers[(d, shift_type)] = int(round(day_shift_data['min_turno'].values[0]))
                                    max_workers[(d, shift_type)] = int(round(day_shift_data['max_turno'].values[0]))

            logger.info(f"Processing estimativas data with {len(matriz_estimativas_gd)} records")
            logger.info(f"  - pess_obj: {len(pess_obj)} entries")
            logger.info(f"  - min_workers: {len(min_workers)} entries")
            logger.info(f"  - max_workers: {len(max_workers)} entries")
        else:
            logger.warning("No estimativas data found, using default values for pess_obj, min_workers, max_workers, and working_shift_2")
               



        # =================================================================
        # 12. ADDITIONAL WORKER ASSIGNMENTS
        # =================================================================
        logger.info("Setting up additional worker assignments")
        
        worker_week_shift = {}

        # Iterate over each worker
        for w in workers:
            for week in range(1, 53):  # Iterate over the 52 weeks
                worker_week_shift[(w, week, 'M')] = 0
                worker_week_shift[(w, week, 'T')] = 0
                
                # Iterate through days of the week for the current week
                for day in week_to_days[week]:
                    if day in non_holidays:  # Make sure we're only checking non-holiday days
                        
                        # Get the rows for the current week and day
                        shift_entries = matriz_calendario_gd[(matriz_calendario_gd['data'].dt.isocalendar().week == week) & (matriz_calendario_gd['data'].dt.day_of_year == day) & (matriz_calendario_gd['colaborador'] == w)]
                        
                        # Check for morning shifts ('M') for the current worker
                        if not shift_entries[shift_entries['tipo_turno'] == "M"].empty:
                            # Assign morning shift to the worker for that week
                            worker_week_shift[(w, week, 'M')] = 1  # Set to 1 if morning shift is found

                        # Check for afternoon shifts ('T') for the current worker
                        if not shift_entries[shift_entries['tipo_turno'] == "T"].empty:
                            # Assign afternoon shift to the worker for that week
                            worker_week_shift[(w, week, 'T')] = 1  # Set to 1 if afternoon shift is found

            if not worker_week_shift:
                logger.warning(f"No week shifts found for worker {w}, this may indicate an issue with the data.")
            

        working_shift_2 = ["M", "T"]

        logger.info("[OK] Data processing completed successfully")
        
        # =================================================================
        # 13. RETURN ALL PROCESSED data
        # =================================================================
        return (
            matriz_calendario_gd,    # 0
            days_of_year,           # 1
            sundays,                # 2
            holidays,               # 3
            special_days,           # 4
            closed_holidays,        # 5
            empty_days,             # 6
            worker_holiday,         # 7
            missing_days,           # 8
            working_days,           # 9
            non_holidays,           # 10
            start_weekday,          # 11
            week_to_days,           # 12
            worker_week_shift,      # 13
            matriz_colaborador_gd,  # 14
            workers,                # 15
            contract_type,          # 16
            total_l,                # 17
            total_l_dom,            # 18
            c2d,                    # 19
            c3d,                    # 20
            l_d,                    # 21
            l_q,                    # 22
            cxx,                    # 23
            t_lq,                   # 24
            tc,                     # 25
            matriz_estimativas_gd,  # 26
            pess_obj,                # 27
            min_workers,            # 28
            max_workers,            # 29
            working_shift_2         # 30
        )
        
    except Exception as e:
        logger.error(f"Error in read_data_alcampo: {e}", exc_info=True)
        raise


