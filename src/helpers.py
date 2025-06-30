"""
Calendar matrix helper functions for the DescansosDataModel.
Converted from R functions for processing employee schedules, holidays, and absences.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional, Tuple

# Local stuff
from src.oracle_config import ORACLE_CONFIG
from src.config import PROJECT_NAME
from base_data_project.log_config import get_logger

# Set up logger
logger = get_logger(PROJECT_NAME)

def get_oracle_url_cx():
    """Create Oracle connection URL for cx_Oracle driver"""
    return (f"oracle+cx_oracle://{ORACLE_CONFIG['username']}:"
            f"{ORACLE_CONFIG['password']}@"
            f"{ORACLE_CONFIG['host']}:{ORACLE_CONFIG['port']}/"
            f"?service_name={ORACLE_CONFIG['service_name']}")

def insert_feriados(df_feriados: pd.DataFrame, reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R insert_feriados function to Python.
    Insert holidays into the schedule matrix.
    
    Args:
        df_feriados: DataFrame with holiday data (columns: data, tipo)
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with holidays inserted
    """
    try:
        # Create new row for TIPO_DIA
        new_row = ['-'] * reshaped_final_3.shape[1]
        new_row[0] = "TIPO_DIA"
        
        # Split matrix into upper and lower parts
        upper_bind = reshaped_final_3.iloc[[0]].copy()
        lower_bind = reshaped_final_3.iloc[1:].copy()
        
        # Create new row DataFrame
        new_row_df = pd.DataFrame([new_row], columns=reshaped_final_3.columns)
        
        # Combine parts
        reshaped_final_3 = pd.concat([upper_bind, new_row_df, lower_bind], ignore_index=True)
        
        # Process each holiday
        for _, holiday_row in df_feriados.iterrows():
            temp = str(holiday_row['data'])
            data = temp[:10]  # Extract date (YYYY-MM-DD format)
            val = holiday_row['tipo']
            
            # Find column indices for this date
            col_indices = []
            for col_idx, col_data in reshaped_final_3.items():
                if data in col_data.values:
                    col_indices.append(col_idx)
            
            if len(col_indices) >= 2:
                if val == 2:
                    # Open holiday - mark both shifts as F
                    reshaped_final_3.iloc[1, col_indices[0]:col_indices[1]+1] = "F"
                elif val == 3:
                    # Closed holiday - mark day type and all employees as F
                    reshaped_final_3.iloc[1, col_indices[0]:col_indices[1]+1] = "F"
                    reshaped_final_3.iloc[3:, col_indices[0]] = "F"
                    reshaped_final_3.iloc[3:, col_indices[1]] = "F"
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in insert_feriados: {str(e)}")
        return reshaped_final_3

def insert_closed_days(closed_days: pd.DataFrame, reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R insert_closedDays function to Python.
    Insert closed days into the schedule matrix.
    
    Args:
        closed_days: DataFrame with closed day data
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with closed days inserted
    """
    try:
        # Process each closed day
        for _, closed_day_row in closed_days.iterrows():
            temp = str(closed_day_row.iloc[0])
            data = temp[:10]  # Extract date (YYYY-MM-DD format)
            
            # Find column indices for this date
            col_indices = []
            for col_idx, col_data in reshaped_final_3.items():
                if data in col_data.values:
                    col_indices.append(col_idx)
            
            if len(col_indices) >= 2:
                # Mark all employees as L (closed) for both shifts
                reshaped_final_3.iloc[3:, col_indices[0]] = "L"
                reshaped_final_3.iloc[3:, col_indices[1]] = "L"
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in insert_closed_days: {str(e)}")
        return reshaped_final_3

def insert_holidays_absences(employees_tot: List[str], ausencias_total: pd.DataFrame, 
                           reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R insert_holidays_abscences function to Python.
    Insert holidays (V) and absences (A) into the schedule matrix.
    
    Args:
        employees_tot: List of all employee IDs
        ausencias_total: DataFrame with absence data
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with absences inserted
    """
    try:
        for colab in employees_tot:
            colab_pad = colab
            
            # Filter absences for this employee
            ausencias = ausencias_total[ausencias_total['matricula'] == colab_pad]
            
            if len(ausencias) == 0:
                continue
            
            # Find employee row index
            row_indices = []
            for row_idx, row_data in reshaped_final_3.iterrows():
                if colab in row_data.values:
                    row_indices.append(row_idx)
            
            if not row_indices:
                continue
                
            row_index = row_indices[0]
            
            # Process each absence
            for _, absence_row in ausencias.iterrows():
                temp = str(absence_row['data_ini'])
                data = temp[:10]  # Extract date
                val = absence_row['tipo_ausencia']
                fk_motivo_ausencia = int(absence_row['fk_motivo_ausencia']) 
                
                # Find column indices for this date
                col_indices = []
                for col_idx, col_data in reshaped_final_3.items():
                    if data in col_data.values:
                        col_indices.append(col_idx)
                
                #logger.info(f"DEBUG: col_indices: {col_indices}")
                if len(col_indices) >= 2:
                    #logger.info(f"DEBUG: fk_motivo_ausencia: {fk_motivo_ausencia}, type: {type(fk_motivo_ausencia)}")
                    if fk_motivo_ausencia == 1:
                        # Vacation
                        reshaped_final_3.iloc[row_index, col_indices[0]] = "V"
                        reshaped_final_3.iloc[row_index, col_indices[1]] = "V"
                    else:
                        # Other absence
                        reshaped_final_3.iloc[row_index, col_indices[0]] = val
                        reshaped_final_3.iloc[row_index, col_indices[1]] = val
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in insert_holidays_absences: {str(e)}")
        return reshaped_final_3

def create_m0_0t(reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R create_M0_0T function to Python.
    Assign 0 after M shift and before T shift to indicate free periods.
    
    Args:
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with 0s for free periods
    """
    try:
        # Iterate through columns in pairs (each date has two columns: M and T)
        for i in range(1, reshaped_final_3.shape[1] - 1, 2):  # Start from 1, step by 2
            # Process each employee row (starting from row 2, index 2)
            for j in range(2, len(reshaped_final_3)):
                current_val = str(reshaped_final_3.iloc[j, i])
                
                if current_val == "M":
                    # Morning shift - set afternoon to 0
                    reshaped_final_3.iloc[j, i + 1] = 0
                elif current_val in ["T", "T1", "T2"]:
                    # Afternoon/Evening shift - set morning to 0
                    reshaped_final_3.iloc[j, i] = 0
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in create_m0_0t: {str(e)}")
        return reshaped_final_3

def create_mt_mtt_cycles(df_alg_variables_filtered: pd.DataFrame, reshaped_final_3: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R create_MT_MTT_cycles function to Python.
    Create MT or MTT cycles according to shift patterns.
    
    Args:
        df_alg_variables_filtered: DataFrame with employee algorithm variables
        reshaped_final_3: Schedule matrix DataFrame
        
    Returns:
        Updated schedule matrix with MT/MTT cycles
    """
    try:
        # Reset column names and row names
        reshaped_final_3.columns = range(reshaped_final_3.shape[1])
        reshaped_final_3.reset_index(drop=True, inplace=True)
        
        # Select required columns
        df_alg_variables_filtered = df_alg_variables_filtered[['emp', 'seq_turno', 'semana_1']].copy()
        
        for _, emp_row in df_alg_variables_filtered.iterrows():
            emp = emp_row['emp']
            seq_turno = emp_row['seq_turno']
            
            # Handle missing seq_turno
            if pd.isna(seq_turno) or seq_turno is None:
                logger.warning(f"No seq_turno defined for employee: {emp}")
                seq_turno = "T"
            
            semana1 = emp_row['semana_1']
            
            # Calculate days in week (simplified - you may need to adjust)
            if len(reshaped_final_3.columns) > 1:
                first_date_str = str(reshaped_final_3.iloc[0, 1])
                eachrep = count_days_in_week(first_date_str) * 2
            else:
                eachrep = 14
            
            #logger.info(f"DEBUG: eachrep: {eachrep}")
            #logger.info(f"DEBUG: seq_turno: {seq_turno}")
            #logger.info(f"DEBUG: semana1: {semana1}")

            # Generate shift patterns based on seq_turno and semana1
            if seq_turno == "MT" and semana1 in ["T", "T1"]:
                new_row = ['T'] * eachrep
                new_row2 = (['M'] * 14 + ['T'] * 14) * ((reshaped_final_3.shape[1] // 2 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MT" and semana1 in ["M", "M1"]:
                new_row = ['M'] * eachrep
                new_row2 = (['T'] * 14 + ['M'] * 14) * ((reshaped_final_3.shape[1] // 2 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MTT" and semana1 in ["M", "M1"]:
                new_row = ['M'] * eachrep
                new_row2 = (['T'] * 14 + ['T'] * 14 + ['M'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MTT" and semana1 == "T1":
                new_row = ['T'] * eachrep
                new_row2 = (['T'] * 14 + ['M'] * 14 + ['T'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MTT" and semana1 == "T2":
                new_row = ['T'] * eachrep
                new_row2 = (['M'] * 14 + ['T'] * 14 + ['T'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MMT" and semana1 == "M1":
                new_row = ['M'] * eachrep
                new_row2 = (['M'] * 14 + ['T'] * 14 + ['M'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MMT" and semana1 == "M2":
                new_row = ['M'] * eachrep
                new_row2 = (['T'] * 14 + ['M'] * 14 + ['M'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            elif seq_turno == "MMT" and semana1 in ["T", "T1"]:
                new_row = ['T'] * eachrep
                new_row2 = (['M'] * 14 + ['M'] * 14 + ['T'] * 14) * ((reshaped_final_3.shape[1] // 3 // 14) + 1)
                new_row = [emp] + new_row + new_row2
                
            else:
                # Default case
                new_row = [seq_turno] * reshaped_final_3.shape[1]
                new_row = [emp] + new_row[1:]

            #logger.info(f"DEBUG: new_row: {new_row}")

            
            # Trim to match matrix width
            elements_to_drop = len(new_row) - reshaped_final_3.shape[1]
            if elements_to_drop > 0:
                new_row = new_row[:len(new_row) - elements_to_drop]
            elif elements_to_drop < 0:
                new_row.extend(['-'] * abs(elements_to_drop))
            
            # Add row to matrix
            new_row_df = pd.DataFrame([new_row], columns=reshaped_final_3.columns)
            reshaped_final_3 = pd.concat([reshaped_final_3, new_row_df], ignore_index=True)
            #logger.info(f"DEBUG: new_row after concat: {new_row}")
            #logger.info(f"DEBUG: elements_to_drop after concat: {elements_to_drop}")
        
        # Reset column and row names
        reshaped_final_3.columns = range(reshaped_final_3.shape[1])
        reshaped_final_3.reset_index(drop=True, inplace=True)
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in create_mt_mtt_cycles: {str(e)}")
        return reshaped_final_3

def assign_days_off(reshaped_final_3: pd.DataFrame, df_daysoff_final: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R assign_days_off function to Python.
    Assign days off to employees in the schedule matrix.
    
    Args:
        reshaped_final_3: Schedule matrix DataFrame
        df_daysoff_final: DataFrame with days off data
        
    Returns:
        Updated schedule matrix with days off assigned
    """
    try:
        emps = df_daysoff_final['employee_id'].unique()
        
        for emp in emps:
            df_daysoff = df_daysoff_final[df_daysoff_final['employee_id'] == emp]
            
            for _, dayoff_row in df_daysoff.iterrows():
                date_temp = str(dayoff_row['schedule_dt'])
                date = date_temp[:10]
                val = dayoff_row['sched_subtype']
                
                # Find employee row index
                row_indices = []
                for row_idx, row_data in reshaped_final_3.iterrows():
                    if str(emp) in row_data.values:
                        row_indices.append(row_idx)
                
                if not row_indices:
                    continue
                    
                row_index = row_indices[0]
                
                # Find column indices for this date
                col_indices = []
                for col_idx, col_data in reshaped_final_3.items():
                    if date in col_data.values:
                        col_indices.append(col_idx)
                
                if len(col_indices) >= 2:
                    reshaped_final_3.iloc[row_index, col_indices[0]] = val
                    reshaped_final_3.iloc[row_index, col_indices[1]] = val
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in assign_days_off: {str(e)}")
        return reshaped_final_3

def assign_empty_days(df_tipo_contrato: pd.DataFrame, reshaped_final_3: pd.DataFrame,
                     not_in_pre_ger: List[str], df_feriados_filtered: pd.DataFrame) -> pd.DataFrame:
    """
    Convert R assign_empty_days function to Python.
    Assign empty days based on contract types.
    
    Args:
        df_tipo_contrato: DataFrame with contract type information
        reshaped_final_3: Schedule matrix DataFrame
        not_in_pre_ger: List of employees not in pre-generated schedules
        df_feriados_filtered: DataFrame with filtered holiday data
        
    Returns:
        Updated schedule matrix with empty days assigned
    """
    try:
        weekday_contrato2 = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        weekday_contrato3 = ['Monday', 'Tuesday', 'Wednesday', 'Thursday']
        absence_types = ['A', 'AP', 'V']
        
        for emp in not_in_pre_ger:
            # Get contract type for this employee
            emp_contract = df_tipo_contrato[df_tipo_contrato['emp'] == emp]
            if len(emp_contract) == 0:
                continue
                
            tipo_de_contrato = emp_contract['tipo_contrato'].iloc[0]
            
            logger.info(f"Colab: {emp}, Tipo de Contrato: {tipo_de_contrato}")
            
            # Find employee row index
            row_indices = []
            for row_idx, row_data in reshaped_final_3.iterrows():
                if emp in row_data.values:
                    row_indices.append(row_idx)
            
            if not row_indices:
                continue
                
            row_index = row_indices[0]
            
            # Skip if contract type is 6
            if tipo_de_contrato == 6:
                logger.info("Tipo de contrato = 6, do nothing, next emp (loop)")
                continue
            
            # Process each date column (step by 2 for M/T pairs)
            for i in range(1, reshaped_final_3.shape[1] - 1, 2):
                date_temp = str(reshaped_final_3.iloc[0, i])
                date = date_temp[:10]
                
                try:
                    weekday = pd.to_datetime(date).day_name()
                    type_of_day = str(reshaped_final_3.iloc[0, i])
                    
                    # Check holiday type
                    holiday_matches = df_feriados_filtered[
                        df_feriados_filtered['data'] == pd.to_datetime(type_of_day)
                    ]
                    type_of_hol = holiday_matches['tipo'].iloc[0] if len(holiday_matches) > 0 else '-'
                    
                    assigned_value = str(reshaped_final_3.iloc[row_index, i])
                    
                    # Apply contract type rules
                    if type_of_hol == 3:
                        # Closed holiday
                        reshaped_final_3.iloc[row_index, i:i+2] = 'F'
                    elif (tipo_de_contrato == 3 and 
                          weekday in weekday_contrato3 and 
                          type_of_hol != 2 and 
                          assigned_value not in absence_types):
                        reshaped_final_3.iloc[row_index, i:i+2] = '-'
                    elif (tipo_de_contrato == 2 and 
                          weekday in weekday_contrato2 and 
                          type_of_hol != 2 and 
                          assigned_value not in absence_types):
                        reshaped_final_3.iloc[row_index, i:i+2] = '-'
                        
                except Exception as date_error:
                    logger.warning(f"Error processing date {date}: {str(date_error)}")
                    continue
        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in assign_empty_days: {str(e)}")
        return reshaped_final_3

def add_trads_code(df_cycle90_info_filtered: pd.DataFrame, lim_sup_manha: str, lim_inf_tarde: str) -> pd.DataFrame:
    """
    Convert R add_trads_code function to Python.
    Add TRADS codes to 90-day cycle information.
    
    Args:
        df_cycle90_info_filtered: DataFrame with 90-day cycle information
        lim_sup_manha: Morning limit time
        lim_inf_tarde: Afternoon limit time
        
    Returns:
        DataFrame with TRADS codes added
    """
    try:
        # Convert time columns to datetime
        time_cols = ['hora_ini_1', 'hora_ini_2', 'hora_fim_1', 'hora_fim_2']
        for col in time_cols:
            if col in df_cycle90_info_filtered.columns:
                df_cycle90_info_filtered[col] = pd.to_datetime(
                    df_cycle90_info_filtered[col], format="%Y-%m-%d %H:%M:%S", errors='coerce'
                )
        
        # Convert limit times
        lim_sup_manha = pd.to_datetime(lim_sup_manha, format="%Y-%m-%d %H:%M:%S", errors='coerce')
        
        # Calculate interval and max exit time
        df_cycle90_info_filtered['intervalo'] = np.where(
            df_cycle90_info_filtered['hora_ini_2'].isna(),
            0,
            (df_cycle90_info_filtered['hora_ini_2'] - df_cycle90_info_filtered['hora_fim_1']).dt.total_seconds() / 3600
        )
        
        df_cycle90_info_filtered['max_exit'] = (
            df_cycle90_info_filtered[['hora_ini_1', 'hora_fim_1', 'hora_ini_2', 'hora_fim_2']].max(axis=1) - 
            pd.Timedelta(minutes=15)
        )

        # Calculate intervalo column
        #df_cycle90_info_filtered['intervalo'] = df_cycle90_info_filtered.apply(
        #    lambda row: 0 if pd.isna(row['hora_ini_2']) else 
        #    (pd.to_datetime(row['hora_ini_2']) - pd.to_datetime(row['hora_fim_1'])).total_seconds() / 3600,
        #    axis=1
        #)
#
        # Calculate max_exit column
        #time_columns = ['hora_ini_1', 'hora_fim_1', 'hora_ini_2', 'hora_fim_2']
        #df_cycle90_info_filtered['max_exit'] = df_cycle90_info_filtered[time_columns].apply(
        #    lambda row: pd.to_datetime(row.dropna()).max() - pd.Timedelta(minutes=15),
        #    axis=1
        #)
        
        # Apply TRADS code logic
        def get_trads_code(row):
            tipo_dia = row['tipo_dia']
            descanso = row['descanso']
            horario_ind = row['horario_ind']
            dia_semana = row['dia_semana']
            intervalo = row['intervalo']
            max_exit = row['max_exit']
            
            if tipo_dia == 'F' and (dia_semana == 1 or dia_semana == 8):
                return 'L_DOM'
            elif tipo_dia == 'F':
                return 'L'
            elif tipo_dia == 'A' and descanso == 'A' and horario_ind == 'N':
                return 'MoT'
            elif tipo_dia == 'A' and descanso == 'A' and horario_ind == 'S' and max_exit >= lim_sup_manha:
                return 'T'
            elif tipo_dia == 'A' and descanso == 'A' and horario_ind == 'S' and max_exit < lim_sup_manha:
                return 'M'
            elif tipo_dia == 'S':
                return '-'
            elif tipo_dia == 'A' and (descanso == 'R' or descanso == 'N') and intervalo >= 1:
                return 'P'
            elif tipo_dia == 'A' and (descanso == 'R' or descanso == 'N') and intervalo < 1 and max_exit >= lim_sup_manha:
                return 'T'
            elif tipo_dia == 'A' and (descanso == 'R' or descanso == 'N') and intervalo < 1 and max_exit < lim_sup_manha:
                return 'M'
            elif tipo_dia == 'A' and descanso == 'A' and horario_ind == 'Y' and intervalo < 1 and max_exit >= lim_sup_manha:
                return 'T'
            elif tipo_dia == 'A' and descanso == 'A' and horario_ind == 'Y' and intervalo < 1 and max_exit < lim_sup_manha:
                return 'M'
            elif tipo_dia == 'N':
                return 'NL'
            else:
                return '-'
        
        df_cycle90_info_filtered['codigo_trads'] = df_cycle90_info_filtered.apply(get_trads_code, axis=1)
        
        return df_cycle90_info_filtered
        
    except Exception as e:
        logger.error(f"Error in add_trads_code: {str(e)}")
        return df_cycle90_info_filtered

def assign_90_cycles(reshaped_final_3: pd.DataFrame, df_cycle90_info_filtered: pd.DataFrame,
                    colab: int, matriz_festivos: pd.DataFrame, lim_sup_manha: str, lim_inf_tarde: str,
                    day: str, reshaped_col_index: list, reshaped_row_index: list, matricula: str) -> pd.DataFrame:
    """
    Convert R assign_90_cycles function to Python.
    Assign 90-day cycles to the schedule matrix.
    
    Args:
        reshaped_final_3: Schedule matrix DataFrame
        df_cycle90_info_filtered: Filtered 90-day cycle information
        colab: Employee ID
        matriz_festivos: Holiday matrix
        lim_sup_manha: Morning limit time
        lim_inf_tarde: Afternoon limit time
        day: Current day string
        reshaped_col_index: Column index in matrix
        reshaped_row_index: Row index in matrix
        matricula: Employee matricula
        
    Returns:
        Updated schedule matrix with 90-day cycles assigned
    """
    try:
        # Convert time limits
        lim_sup_manha = f"2000-01-01 {lim_sup_manha}"
        lim_sup_manha = pd.to_datetime(lim_sup_manha, format="%Y-%m-%d %H:%M")
        lim_inf_tarde = f"2000-01-01 {lim_inf_tarde}"
        lim_inf_tarde = pd.to_datetime(lim_inf_tarde, format="%Y-%m-%d %H:%M")
        #logger.info(f"DEBUG: lim_sup_manha: {lim_sup_manha}")
        #logger.info(f"DEBUG: lim_inf_tarde: {lim_inf_tarde}")
        
        # Add TRADS codes
        df_cycle90_info_filtered = add_trads_code(df_cycle90_info_filtered, 
                                                 lim_sup_manha.strftime("%Y-%m-%d %H:%M:%S"), 
                                                 lim_inf_tarde.strftime("%Y-%m-%d %H:%M:%S"))
        
        #logger.info(f"DEBUG: df_cycle90_info_filtered: {df_cycle90_info_filtered}")

        # Reset row names
        reshaped_final_3.reset_index(drop=True, inplace=True)
        
        # Get holidays as list of strings
        # TODO: remove non used variables. during code convertion we didnt find the need for this variable
        #festivos = [str(date) for date in matriz_festivos['data']]
        
        # Process the specific day range
        if isinstance(reshaped_col_index, list) and len(reshaped_col_index) >= 2:
            col_range = [reshaped_col_index[0], reshaped_col_index[1]]
        else:
            col_range = [reshaped_col_index]

        #logger.info(f"DEBUG: col_range: {col_range}")
        
        for k in col_range:
            
            day_number = pd.to_datetime(day).weekday() + 1  # Convert to 1-7 format
            
            # Find matching cycle row for this day
            cycle_rows = df_cycle90_info_filtered[
                df_cycle90_info_filtered['schedule_day'].dt.strftime('%Y-%m-%d') == day
            ]

            #logger.info(f"DEBUG: cycle_rows: {cycle_rows}")
            #logger.info(f"DEBUG: k: {k}")
            
            if len(cycle_rows) > 0:
                cycle_row = cycle_rows.iloc[0]
                val = cycle_row.get('codigo_trads', '-')
                reshaped_final_3.iloc[reshaped_row_index, k] = val

        
        return reshaped_final_3
        
    except Exception as e:
        logger.error(f"Error in assign_90_cycles: {str(e)}")
        return reshaped_final_3

def load_pre_ger_scheds(df_pre_ger: pd.DataFrame, employees_tot: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    """
    Convert R load_pre_ger_scheds function to Python.
    Load pre-generated schedules.
    
    Args:
        df_pre_ger: DataFrame with pre-generated schedule data
        employees_tot: List of all employees
        
    Returns:
        Tuple of (reshaped schedule matrix, list of employees with pre-generated schedules)
    """
    try:
        if len(df_pre_ger) == 0:
            return pd.DataFrame(), []
        
        # Rename first column
        df_pre_ger.columns = ['employee_id'] + list(df_pre_ger.columns[1:])
        
        # Get employees with pre-generated schedules
        emp_pre_ger = df_pre_ger['employee_id'].unique().tolist()
        
        # Filter for 'P' indicator
        df_pre_ger_filtered = df_pre_ger[
            df_pre_ger['ind'] == 'P'
        ].drop('ind', axis=1)
        
        # Pivot wider
        reshaped = df_pre_ger_filtered.pivot_table(
            index='employee_id', 
            columns='schedule_dt', 
            values='sched_subtype', 
            aggfunc='first'
        ).reset_index()
        
        # Create column names row
        column_names = pd.DataFrame([reshaped.columns.tolist()], columns=reshaped.columns)
        column_names.iloc[0, 0] = "Dia"
        
        # Convert employee_id to string
        reshaped['employee_id'] = reshaped['employee_id'].astype(str)
        
        # Combine column names with data
        reshaped_names = pd.concat([column_names, reshaped], ignore_index=True)
        
        # Duplicate columns to get M/T shifts
        first_col = reshaped_names.iloc[:, [0]]
        last_cols = reshaped_names.iloc[:, 1:]
        
        # Duplicate last columns
        duplicated_cols = pd.concat([last_cols, last_cols], axis=1)
        
        # Sort columns by name
        duplicated_cols = duplicated_cols.reindex(sorted(duplicated_cols.columns), axis=1)
        
        # Combine first column with duplicated columns
        reshaped_final = pd.concat([first_col, duplicated_cols], axis=1)
        
        # Reset column and row names
        reshaped_final.columns = range(reshaped_final.shape[1])
        reshaped_final.reset_index(drop=True, inplace=True)
        
        # Create TURNO row
        new_row = ['M' if i % 2 == 1 else 'T' for i in range(reshaped_final.shape[1])]
        new_row[0] = "TURNO"
        
        # Trim to match matrix width
        elements_to_drop = len(new_row) - reshaped_final.shape[1]
        if elements_to_drop > 0:
            new_row = new_row[:len(new_row) - elements_to_drop]
        
        # Combine first row, TURNO row, and remaining rows
        reshaped_final_1 = reshaped_final.iloc[[0]]
        new_row_df = pd.DataFrame([new_row], columns=reshaped_final.columns)
        reshaped_final_2 = reshaped_final.iloc[1:]
        
        reshaped_final_3 = pd.concat([reshaped_final_1, new_row_df, reshaped_final_2], ignore_index=True)
        reshaped_final_3.columns = range(reshaped_final_3.shape[1])
        
        return reshaped_final_3, emp_pre_ger
        
    except Exception as e:
        logger.error(f"Error in load_pre_ger_scheds: {str(e)}")
        return pd.DataFrame(), []

def count_days_in_week(date_str: str) -> int:
    """
    Helper function to count days in a week.
    Simplified implementation - you may need to adjust based on business logic.
    
    Args:
        date_str: Date string
        
    Returns:
        Number of days (default 7)
    """
    # Ensure the date is a datetime object
    if isinstance(date_str, str):
        date_str = pd.to_datetime(date_str)
    elif isinstance(date_str, datetime):
        date_str = pd.to_datetime(date_str)
    
    # Convert pandas weekday (0=Monday, 6=Sunday) to R's wday (1=Sunday, 7=Saturday)
    pandas_weekday = date_str.weekday()
    r_weekday = 1 if pandas_weekday == 6 else pandas_weekday + 2
    
    # If the date is a Sunday (wday=1), move to the previous Monday
    if r_weekday == 1:
        start_of_week = date_str - timedelta(days=6)
    else:
        # Otherwise, find the Monday of the given week
        start_of_week = date_str - timedelta(days=r_weekday - 2)
    
    # Find the Sunday of the given week
    end_of_week = start_of_week + timedelta(days=6)
    
    # Generate a sequence of dates from Monday to Sunday
    week_days = pd.date_range(start=start_of_week, end=end_of_week, freq='D')
    
    # Count the number of days from the given date onwards
    num_days = len(week_days[week_days >= date_str])
    
    return num_days

def load_wfm_scheds(df_pre_ger: pd.DataFrame, employees_tot_pad: List[str]) -> Tuple[pd.DataFrame, List[str], pd.DataFrame]:
    """
    Convert R load_WFM_scheds function to Python - simplified version.
    """
    try:
        if len(df_pre_ger) == 0:
            return pd.DataFrame(), [], pd.DataFrame()
        
        # Basic processing
        df_pre_ger = df_pre_ger.copy()
        df_pre_ger.columns = ['employee_id'] + list(df_pre_ger.columns[1:])
        
        # Convert WFM types to TRADS and get unique employees
        df_pre_ger = convert_types_in(df_pre_ger)
        emp_pre_ger = df_pre_ger['employee_id'].unique().tolist()
        
        # Fill missing dates and pivot to matrix format
        df_pre_ger['schedule_dt'] = pd.to_datetime(df_pre_ger['schedule_dt']).dt.strftime('%Y-%m-%d')
        df_pre_ger['sched_subtype'] = df_pre_ger['sched_subtype'].fillna('-')
        
        # Count days off
        df_count = df_pre_ger.groupby('employee_id')['sched_subtype'].apply(
            lambda x: (x.isin(['L', 'LD', 'LQ', 'F', 'V', '-'])).sum()
        ).reset_index(name='days_off_count')
        
        # Use the same reshaping logic as load_pre_ger_scheds
        reshaped_final_3, _ = load_pre_ger_scheds(df_pre_ger, employees_tot_pad)
        
        return reshaped_final_3, emp_pre_ger, df_count
        
    except Exception as e:
        logger.error(f"Error in load_wfm_scheds: {str(e)}")
        return pd.DataFrame(), [], pd.DataFrame()

def convert_types_in(df: pd.DataFrame) -> pd.DataFrame:
    """Convert WFM types to TRADS - simple mapping."""
    type_map = {
        ('T', 'M'): 'M', ('T', 'T'): 'T', ('T', 'H'): 'MoT', ('T', 'P'): 'P',
        ('F', None): 'L', ('F', 'D'): 'LD', ('F', 'Q'): 'LQ', ('F', 'C'): 'C',
        ('R', None): 'F', ('N', None): '-', ('T', 'A'): 'V'
    }
    
    df['sched_subtype'] = df.apply(
        lambda row: type_map.get((row.get('type'), row.get('subtype')), '-'), axis=1
    )
    df['ind'] = 'P'
    return df

def count_dates_per_year(start_date_str: str, end_date_str: str) -> str:
    """
    Convert R count_dates_per_year function to Python.
    Count dates per year in a date range and return the year with most dates.
    
    Args:
        start_date_str: Start date as string (YYYY-MM-DD format)
        end_date_str: End date as string (YYYY-MM-DD format)
        
    Returns:
        Year with the most dates as string
    """
    try:
        # Convert input strings to date format
        start_date = pd.to_datetime(start_date_str)
        end_date = pd.to_datetime(end_date_str)
        
        # Generate sequence of dates
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Extract the unique years from the date sequence
        years = dates.year.unique()
        
        # Initialize a dictionary to store the count of dates for each year
        year_counts = {}
        
        # Count the number of dates for each year
        for year in years:
            year_counts[str(year)] = (dates.year == year).sum()
        
        # Display the counts for each year
        logger.info(f"Date counts per year: {year_counts}")
        
        # Determine which year has the most dates
        year_with_most_dates = max(year_counts.keys(), key=lambda x: year_counts[x])
        
        # Output the year with the most dates
        logger.info(f"Year with most dates is: {year_with_most_dates}")
        
        return year_with_most_dates
        
    except Exception as e:
        logger.error(f"Error in count_dates_per_year: {str(e)}")
        # Return current year as fallback
        return str(pd.Timestamp.now().year)

def get_limit_mt(matricula: str, df_colaborador: pd.DataFrame) -> Tuple[str, str]:
    """
    Get MT (Morning/Afternoon) time limits for a specific employee from df_colaborador.
    
    Args:
        matricula: Employee matricula (ID)
        df_colaborador: DataFrame containing employee data with limit columns
        
    Returns:
        Tuple of (lim_sup_manha, lim_inf_tarde) as time strings
    """
    try:
        # Filter df_colaborador for this specific employee
        employee_data = df_colaborador[df_colaborador['matricula'] == matricula]
        
        if len(employee_data) == 0:
            logger.warning(f"No employee data found for matricula {matricula}")
            # Return default values
            return "12:00", "14:00"
        
        # Get the first matching record
        emp_record = employee_data.iloc[0]
        
        # Extract the limit columns
        lim_sup_manha = str(emp_record.get('limite_superior_manha', '12:00'))
        lim_inf_tarde = str(emp_record.get('limite_inferior_tarde', '14:00'))
        
        # Handle potential None or NaN values
        if pd.isna(lim_sup_manha) or lim_sup_manha == 'nan':
            lim_sup_manha = "12:00"
        if pd.isna(lim_inf_tarde) or lim_inf_tarde == 'nan':
            lim_inf_tarde = "14:00"
        
        logger.debug(f"Retrieved MT limits for matricula {matricula}: morning={lim_sup_manha}, afternoon={lim_inf_tarde}")
        
        return lim_sup_manha, lim_inf_tarde
        
    except Exception as e:
        logger.error(f"Error in get_limit_mt for matricula {matricula}: {str(e)}")
        # Return default values in case of error
        return "12:00", "14:00"

def pad_zeros(value: str, length: int = 10) -> str:
    """
    Helper function to pad employee IDs with zeros.
    
    Args:
        value: Value to pad
        length: Total length after padding
        
    Returns:
        Padded string
    """
    try:
        return str(value).zfill(length)
    except Exception as e:
        logger.error(f"Error in pad_zeros: {str(e)}")
        return str(value)
    
def calcular_max(sequencia: List[float]) -> float:
    """
    Helper method to calculate maximum using ocorrencia_A and ocorrencia_B logic.
    """
    try:
        valor_a = ocorrencia_a(sequencia)
        valor_b = ocorrencia_b(sequencia)
        return max(valor_a, valor_b)
    except:
        return max(sequencia) if sequencia else 0

def ocorrencia_a(sequencia: List[float]) -> float:
    """
    Convert R ocorrencia_A function to Python.
    """
    valores_unicos = sorted(set(sequencia), reverse=True)
    n = len(sequencia)
    
    resultado_pass1 = float('-inf')
    
    if n == 1:
        return sequencia[0]
    
    if n == 2:
        return sum(sequencia) / len(sequencia)
    
    # Step 1: Look for 3 consecutive occurrences of the highest value
    for valor in valores_unicos:
        for i in range(n - 2):
            if (sequencia[i] == valor and 
                sequencia[i + 1] == valor and 
                sequencia[i + 2] == valor):
                resultado_pass1 = valor
                break
        if resultado_pass1 != float('-inf'):
            break
    
    # Step 2: If not found, follow second rule
    if len(valores_unicos) < 3:
        x = valores_unicos
    else:
        x = valores_unicos[:3]
    
    melhor_sequencia = None
    melhor_soma = float('-inf')
    resultado_pass2 = float('-inf')
    
    for i in range(n - 2):
        sub_seq = sequencia[i:i + 3]
        if all(val in x for val in sub_seq):
            soma_seq = sum(sub_seq)
            if soma_seq > melhor_soma:
                melhor_soma = soma_seq
                melhor_sequencia = sub_seq
    
    if melhor_sequencia is not None:
        resultado_pass2 = min(melhor_sequencia)
    
    return max(resultado_pass1, resultado_pass2)

def ocorrencia_b(sequencia: List[float]) -> float:
    """
    Convert R ocorrencia_B function to Python.
    """
    valores_unicos = sorted(set(sequencia), reverse=True)
    n = len(sequencia)
    
    if n == 1:
        return sequencia[0]
    
    # Step 1: Calculate the 3 maximum values of the sequence
    if len(valores_unicos) < 3:
        maximos = valores_unicos
    else:
        maximos = valores_unicos[:3]
    
    # Step 2: Check the largest of these maximum values and if there are at least 2 situations of 2 consecutive
    maior_valor = max(maximos)
    contagem_consecutiva = 0
    i = 0
    
    while i <= n - 2:
        if sequencia[i] == maior_valor and sequencia[i + 1] == maior_valor:
            contagem_consecutiva += 1
            i += 2
        else:
            i += 1
    
    if contagem_consecutiva >= 2:
        return maior_valor
    
    # Step 3: If there are not 2 pairs of the highest value, check pairs among the 3 highest values
    pares_validos = []
    i = 0
    while i <= n - 2:
        if sequencia[i] in maximos and sequencia[i + 1] in maximos:
            pares_validos.append(sequencia[i:i + 2])
            i += 2
        else:
            i += 1
    
    if len(pares_validos) >= 2:
        soma_maxima = float('-inf')
        melhor_pares = None
        
        for j in range(len(pares_validos) - 1):
            for k in range(j + 1, len(pares_validos)):
                soma_atual = sum(pares_validos[j]) + sum(pares_validos[k])
                if soma_atual > soma_maxima:
                    soma_maxima = soma_atual
                    melhor_pares = pares_validos[j] + pares_validos[k]
        
        if melhor_pares:
            return min(melhor_pares)
    
    return -1  # Case where there are not enough pairs, return -1

def count_open_holidays(matriz_festivos: pd.DataFrame, tipo_contrato: int) -> List[int]:
    """
    Helper method to count open holidays based on contract type.
    
    Args:
        matriz_festivos: DataFrame with holiday data
        tipo_contrato: Contract type (2 or 3)
        
    Returns:
        List with [l_dom_count, total_working_days]
    """
    try:
        # Convert data column to datetime if not already
        matriz_festivos['data'] = pd.to_datetime(matriz_festivos['data'])
        
        if tipo_contrato == 3:
            # Count holidays Monday to Thursday (weekday 0-3)
            weekday_holidays = matriz_festivos[
                (matriz_festivos['data'].dt.weekday >= 0) & 
                (matriz_festivos['data'].dt.weekday <= 3)
            ]
        elif tipo_contrato == 2:
            # Count holidays Monday to Friday (weekday 0-4)
            weekday_holidays = matriz_festivos[
                (matriz_festivos['data'].dt.weekday >= 0) & 
                (matriz_festivos['data'].dt.weekday <= 4)
            ]
        else:
            weekday_holidays = pd.DataFrame()
        
        l_dom_count = len(weekday_holidays)
        
        # Calculate total working days based on contract type
        # This is a simplified calculation - you may need to adjust based on business rules
        if tipo_contrato == 3:
            total_working_days = 4 * 52  # Approximate: 4 days per week * 52 weeks
        elif tipo_contrato == 2:
            total_working_days = 5 * 52  # Approximate: 5 days per week * 52 weeks
        else:
            total_working_days = 0
            
        return [l_dom_count, total_working_days]
        
    except Exception as e:
        logger.error(f"Error in _count_open_holidays: {str(e)}")
        return [0, 0]
    

def func_turnos(matriz2, tipo):
    """Helper function to process specific shift types (MoT, P, etc.)."""
    # Filter rows that match 'tipo' and update TIPO_TURNO
    matriz2_filtered = matriz2[matriz2['TIPO_TURNO'] == tipo].copy()
    
    if len(matriz2_filtered) > 0:
        # Group by COLABORADOR and DATA, then assign M/T based on row number
        def assign_shift_type(group):
            group = group.copy()
            group.loc[group.index[0], 'TIPO_TURNO'] = 'M'  # First row becomes 'M'
            if len(group) > 1:
                group.loc[group.index[1], 'TIPO_TURNO'] = 'T'  # Second row becomes 'T'
            return group
        
        matriz2_filtered = (matriz2_filtered
                           .groupby(['COLABORADOR', 'DATA'])
                           .apply(assign_shift_type)
                           .reset_index(drop=True))
    
    # Combine the filtered and updated data with the rest of the data
    matriz2_rest = matriz2[matriz2['TIPO_TURNO'] != tipo].copy()
    result = pd.concat([matriz2_rest, matriz2_filtered], ignore_index=True)
    
    return result

def adjusted_isoweek(date):
    """Calculate adjusted ISO week number."""
    import pandas as pd
    
    date = pd.to_datetime(date)
    week = date.isocalendar().week
    month = date.month
    
    # If week is 1 but date is in December
    if week == 1 and month == 12:
        return 53
    return week

def custom_round(x):
    """Custom rounding function."""
    import math
    return math.ceil(x) if (x - math.floor(x)) >= 0.3 else math.floor(x)

def calcular_folgas2(semana_df):
    """Calculate folgas for 2-day contracts."""
    # Filter work days (excluding Sunday)
    dias_trabalho = semana_df[
        (semana_df['WDAY'] != 1) & 
        semana_df['HORARIO'].isin(['H', 'OUT'])
    ]
    
    l_res = 0
    if (len(dias_trabalho[dias_trabalho['DIA_TIPO'] == 'domYf']) > 0 and 
        len(dias_trabalho[(dias_trabalho['WDAY'] == 7) & 
                         (dias_trabalho['HORARIO'] == 'H') & 
                         (dias_trabalho['DIA_TIPO'] != 'domYf')]) > 0):
        l_res = 1
    
    # Identify holidays
    feriados = semana_df[
        (semana_df['DIA_TIPO'] == 'domYf') & 
        (semana_df['WDAY'] != 1) & 
        semana_df['HORARIO'].isin(['H', 'OUT'])
    ]
    
    l_dom = max(len(feriados) - 1, 0) if len(feriados) > 0 else 0
    
    return pd.DataFrame({'L_RES': [l_res], 'L_DOM': [l_dom]})

def calcular_folgas3(semana_df):
    """Calculate folgas for 3-day contracts."""
    # Work days in week
    semana_h = len(semana_df[semana_df['HORARIO'].isin(['H', 'OUT', 'NL3D'])])
    
    if semana_h <= 0:
        return pd.DataFrame({'L_RES': [0], 'L_DOM': [0]})
    
    # Work days excluding Sunday
    dias_trabalho = semana_df[
        (semana_df['WDAY'] != 1) & 
        semana_df['HORARIO'].isin(['H', 'OUT', 'NL3D'])
    ]
    
    # Work days Friday and Saturday
    dias_h = len(dias_trabalho[dias_trabalho['DIA_TIPO'] != 'domYf'])
    l_res = max(min(dias_h, semana_h - 3), 0)
    
    # Identify holidays
    feriados = semana_df[
        (semana_df['DIA_TIPO'] == 'domYf') & 
        (semana_df['WDAY'] != 1)
    ]
    
    l_dom = max(len(feriados) - 2, 0) if len(feriados) > 0 else 0
    
    return pd.DataFrame({'L_RES': [l_res], 'L_DOM': [l_dom]})