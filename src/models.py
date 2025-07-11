"""Data models and operations for the my_new_project project.

This module contains data models, transformation functions, and utility methods
for working with the project's data structures.
"""

import logging
import pandas as pd
import numpy as np
import os
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from isoweek import Week

# Import project-specific components
from src.config import PROJECT_NAME, CONFIG, ROOT_DIR
from src.helpers import (
    calcular_max, count_open_holidays, 
    insert_feriados, insert_closed_days, insert_holidays_absences,
    create_m0_0t, create_mt_mtt_cycles, assign_days_off, assign_empty_days,
    add_trads_code, assign_90_cycles, load_pre_ger_scheds, get_limit_mt,
    count_dates_per_year, load_wfm_scheds, func_turnos, adjusted_isoweek,
    custom_round, calcular_folgas2, calcular_folgas3
)
from src.load_csv_functions.load_valid_emp import load_valid_emp_csv
from base_data_project.algorithms.factory import AlgorithmFactory
from base_data_project.data_manager.managers.base import BaseDataManager
from base_data_project.data_manager.managers.managers import CSVDataManager, DBDataManager
from base_data_project.storage.models import BaseDataModel
from base_data_project.storage.containers import BaseDataContainer
from base_data_project.log_config import get_logger

class DescansosDataModel(BaseDataModel):
    """
    Container for managing and transforming project data.
    
    This class provides a central place for data operations including:
    - Loading and validating data entities
    - Transforming data for analysis
    - Tracking data lineage and operations
    """
    
    def __init__(self, data_container: BaseDataContainer = None, project_name: str = PROJECT_NAME, external_data: Dict[str, Any] = CONFIG.get('defaults_external_data', {})):
        """Initialize an empty data container."""
        # get the logic already implemented in base class
        super().__init__(data_container=data_container, project_name=PROJECT_NAME)

        self.logger = get_logger(PROJECT_NAME)

        self.external_call_data = external_data # consider removing here or in service

        # Important auxiliary data
        # Auxiliary data (TODO: define how it starts)
        self.auxiliary_data = {
            'messages_df': pd.DataFrame(),
            'final': None, # TODO: change the name
            'num_fer_doms': 0,
            'params_algo': None,
            'params_lq': None,
            'valid_emp': None,
            'colabs_id_list': None,
            'convenio': None,
            'unit_id': None,
            'secao_id': None,
            'posto_id_list': None,
            'current_posto_id': None,
            'df_festivos': None,
            'df_closed_days': None,
            'df_turnos': None,
            'df_calendario_passado': None,
            'df_day_aloc': None,
            'emp_pre_ger': None,
            'df_count': None
        }

        # Raw data storage
        self.raw_data = {
            'df_calendario': None,
            'df_colaborador': None,
            'df_estimativas': None
        }

        # Medium data storage
        self.medium_data = {
            'df_calendario': None,
            'df_colaborador': None,
            'df_estimativas': None
        }
        
        # Transformed data
        self.rare_data = {
            'final_boss_give_me_a_name_pls': None, # TODO: define this name
        }

        self.formatted_data = {
            'df_final': None,  # Final output DataFrame
        }
        
        # Metadata for tracking operations
        self.operations_log = []
        
        self.logger.info("DataContainer initialized")
    
    def load_process_data(self, data_manager: BaseDataManager, entities_dict: Dict[str, str] = ['valid_employees']) -> bool:
        """
        Load data from the data manager.
        
        Args:
            data_manager: The data manager instance
            enities: list of entities names to load
            
        Returns:
            True if successful, False otherwise
        """
        # Load messages df, from where?
        try:
            messages_path = os.path.join(ROOT_DIR, 'data', 'csvs', '') # TODO: add the file path
            pass
        except Exception as e:
            self.logger.error()
            return False
        

        try:
            self.logger.info("Loading process data from data manager")

            # entities = ['matriz_A', 'params_LQ'] # TODO: passar para configuração
            
            # Get entities to load from configuration
            if not entities_dict:
                self.logger.warning("No entities passed as argument")
                return False
            
            if isinstance(data_manager, CSVDataManager):
                valid_emp = load_valid_emp_csv()
            elif isinstance(data_manager, DBDataManager):
                # valid emp info
                query_path = entities_dict['valid_emp']
                process_id_str = "'" + str(self.external_call_data['current_process_id']) + "'"
                valid_emp = data_manager.load_data('valid_emp', query_file=query_path, process_id=process_id_str)
            else:
                self.logger.error(f"No instance found for data_manager: {data_manager.__name__}")

            self.logger.info(f"valid_emp: {valid_emp.columns}")

            # Save important this important info to be able to use it on querys
            unit_id = valid_emp['fk_unidade'].unique()[0]  # Get first (and only) unique value
            secao_id = valid_emp['fk_secao'].unique()[0]   # Get first (and only) unique value
            posto_id_list = valid_emp['fk_perfil'].unique().tolist()  # Get list of unique values

            if len(valid_emp['fk_unidade'].unique()) > 1 or len(valid_emp['fk_secao'].unique()) > 1:
                self.logger.error("More than one fk_secao or fk_unidade associated with the process.")
                raise ValueError

            # Get colab_ids list
            colabs_id_list = valid_emp[[]]

            # Get main year between start and end dates
            main_year = count_dates_per_year(start_date_str=self.external_call_data.get('start_date', ''), end_date_str=self.external_call_data.get('end_date', ''))

            # TODO: semanas_restantes logic to add to auxiliary_data

            # Logic needed because query cant run against dfs
            if isinstance(data_manager, CSVDataManager):
                params_lq = data_manager.load_data('params_lq')
            elif isinstance(data_manager, DBDataManager):
                # valid emp info
                query_path = entities_dict['params_lq']
                params_lq = data_manager.load_data('params_lq', query_file=query_path)
            else:
                self.logger.error(f"No instance found for data_manager: {data_manager.__name__}")

            # festivos information
            # TODO: join the other query and make only one df
            query_path = entities_dict['df_festivos']
            unit_id_str = "'" + str(unit_id) + "'"
            df_festivos = data_manager.load_data('df_festivos', query_file=query_path, unit_id=unit_id_str)

            # Copy the dataframes into the apropriate dict
            # TODO: should we ensure unit, secao e posto are only one value?
            self.auxiliary_data['valid_emp'] = valid_emp.copy()
            self.auxiliary_data['params_lq'] = params_lq.copy()
            self.auxiliary_data['df_festivos'] = df_festivos.copy()
            self.auxiliary_data['unit_id'] = unit_id 
            self.auxiliary_data['secao_id'] = secao_id
            self.auxiliary_data['posto_id_list'] = posto_id_list
            self.auxiliary_data['main_year'] = main_year
            self.auxiliary_data['colabs_id_list'] = colabs_id_list

            if not self.raw_data:
                self.logger.warning("No data was loaded")
                return False     
                
            self.logger.info(f"Successfully loaded {len(self.raw_data)} entities")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading process data: {str(e)}")

            return False

    def validate_process_data(self):
        """
        Implement a validation method for process data loading
        """
        return True

    def load_colaborador_info(self, data_manager: BaseDataManager, posto_id: int = 0) -> True:
        """
        transform database data into data raw
        """
        valid_emp = self.auxiliary_data['valid_emp'].copy()
        valid_emp = valid_emp[valid_emp['fk_perfil'] == posto_id]
        colabs_id_list = valid_emp['fk_colaborador'].tolist()
        if len(colabs_id_list) == 0:
            self.logger.error(f"colabs_id_list provided is empty (invalid): {colabs_id_list}")
            return False
        elif len(colabs_id_list) == 1:
            colabs_str = colabs_id_list[0]
        elif len(colabs_id_list) > 1:
            colabs_str = ','.join(colabs_id_list)
        
        try:
            # colaborador info
            query_path = CONFIG.get('available_entities_raw', {}).get('df_colaborador')
            df_colaborador = data_manager.load_data('df_colaborador', query_file=query_path, colabs_id=colabs_str)
            df_colaborador = df_colaborador.rename(columns={'ec.codigo': 'fk_colaborador', 'codigo': 'fk_colaborador'})
            
            # TODO: save the dataframes if they are needed elsewhere, if not let them die here
            self.raw_data['df_colaborador'] = df_colaborador.copy()
            self.auxiliary_data['num_fer_doms'] = 0
            return True
        except Exception as e:
            self.logger.error(f"Error loading colaborador info. Error: {e}")
            return False
        
    def load_estimativas_info(self, data_manager: BaseDataManager, posto_id: int = 0, start_date: str = '', end_date: str = ''):
        """
        Load necessities from data manager and treat them data
        """
        if posto_id == 0:
            self.logger.error(f"posto_id provided is unvaid: {posto_id}")
            return False
        if start_date == '' or end_date == '':
            self.logger.error(f"start_date or end_date provided are empty. start: {start_date}, end_date: {end_date}")
            return False

        try:
            # df_estimativas borns as an empty dataframe
            df_estimativas = pd.DataFrame()

            columns_select = ['nome', 'emp', 'fk_tipo_posto', 'loja', 'secao', 'h_tm_in', 'h_tm_out', 'h_tt_in', 'h_tt_out', 'h_seg_in', 'h_seg_out', 'h_ter_in', 'h_ter_out', 'h_qua_in', 'h_qua_out', 'h_qui_in', 'h_qui_out', 'h_sex_in', 'h_sex_out', 'h_sab_in', 'h_sab_out', 'h_dom_in', 'h_dom_out', 'h_fer_in', 'h_fer_out'] # TODO: define what columns to select

            # Get sql file path, if the cvs is being used, it gets the the path defined on dummy_data_filepaths
            # turnos information: doesnt need a query since is information resent on core_alg_params
            #query_path = CONFIG.get('available_entities_aux', {}).get('df_turnos', '')
            df_turnos = self.raw_data['df_colaborador'].copy()
            df_turnos = df_turnos[columns_select]

            # Estrutura wfm information
            query_path = CONFIG.get('available_entities_aux', {}).get('df_estrutura_wfm', '')
            df_estrutura_wfm = data_manager.load_data('df_estrutura_wfm', query_file=query_path)

            # feriados information
            query_path = CONFIG.get('available_entities_aux', {}).get('df_feriados', '')
            df_feriados = data_manager.load_data('df_feriados', query_file=query_path)

            # faixa horario information
            query_path = CONFIG.get('available_entities_aux', {}).get('df_faixa_horario', '')
            df_faixa_horario = data_manager.load_data('df_faixa_horario', query_file=query_path)

            # orcamento information
            query_path = CONFIG.get('available_entities_aux', {}).get('df_orcamento', '')
            start_date = "'" + start_date + "'"
            end_date = "'" + end_date + "'"
            df_orcamento = data_manager.load_data('df_orcamento', query_file=query_path, posto_id=posto_id, start_date=start_date, end_date=end_date)
            self.logger.info(f"df_orcamento columns: {df_orcamento.columns.tolist()}")

            # granularidade information
            query_path = CONFIG.get('available_entities_aux', {}).get('df_granularidade', '')
            df_granularidade = data_manager.load_data('df_granularidade', query_file=query_path, start_date=start_date, end_date=end_date, posto_id=posto_id)

            # TODO: save the dataframes if they are needed elsewhere, if not let them die here
            self.raw_data['df_estimativas'] = df_estimativas.copy()
            self.auxiliary_data['df_turnos'] = df_turnos.copy()
            self.auxiliary_data['df_estrutura_wfm'] = df_estrutura_wfm.copy()
            self.auxiliary_data['df_feriados'] = df_feriados.copy()
            self.auxiliary_data['df_faixa_horario'] = df_faixa_horario.copy()
            self.auxiliary_data['df_orcamento'] = df_orcamento.copy()
            self.auxiliary_data['df_granularidade'] = df_granularidade.copy()
            return True
        
        except Exception as e:
            self.logger.error(f"Error loading estimativas info data. Error: {e}")
            return False
    
    def load_calendario_info(self, data_manager: BaseDataManager, process_id: int = 0, posto_id: int = 0, start_date: str = '', end_date: str = '', colabs_passado: List[int] = []):
        """
        Load calendario from data manager and treat the data
        """
        if posto_id == 0:
            self.logger.error("posto_id is 0, returning False")
            return False
        
        try:
            # Tipo_contrato info
            df_colaborador = self.raw_data['df_colaborador'].copy()
            self.logger.info(f"df_colaborador shape: {df_colaborador.shape}")
            self.logger.info(f"df_colaborador columns: {df_colaborador.columns.tolist()}")
            
            colaborador_list = df_colaborador['emp'].tolist()
            #df_tipo_contrato = df_colaborador[['emp', 'tipo_contrato']] # TODO: ensure it is needed

            # Get the colab_id for ciclos 90 - FIXED: Added str.upper() and proper column selection
            try:
                colaborador_90_list = df_colaborador[df_colaborador['seq_turno'].str.upper() == 'CICLO']['fk_colaborador'].tolist()
                self.logger.info(f"Found {len(colaborador_90_list)} employees with 90-day cycles")
            except KeyError as e:
                self.logger.error(f"Column not found for 90-day cycles: {e}")
                self.logger.info(f"Available columns: {df_colaborador.columns.tolist()}")
                colaborador_90_list = []
            except Exception as e:
                self.logger.error(f"Error processing 90-day cycles: {e}")
                colaborador_90_list = []

            # Get sql file path, if the cvs is being used, it gets the the path defined on dummy_data_filepaths
            # calendario information
            #query_path = CONFIG.get('available_entities_raw', {}).get('df_calendario', '')
            #df_calendario = data_manager.load_data('df_calendario', custom_query=query_path)
            df_calendario = pd.DataFrame()

            # Calendario passado - FIXED: Added proper type conversion
            try:
                main_year = str(self.auxiliary_data.get('main_year', ''))
                if not main_year:
                    self.logger.warning("main_year not found in auxiliary_data")
                    main_year = pd.to_datetime(start_date).year
                
                first_date_passado = f"{main_year}-01-01"
                self.logger.info(f"first_date_passado: {first_date_passado}")
                
                last_date_passado = pd.to_datetime(self.external_call_data.get('end_date', end_date))
                last_date_passado = last_date_passado + pd.Timedelta(days=7)
                last_date_passado = last_date_passado.strftime('%Y-%m-%d')
                self.logger.info(f"last_date_passado: {last_date_passado}")
                
            except Exception as e:
                self.logger.error(f"Error setting up dates: {e}")
                return False

            df_colaborador = self.raw_data['df_colaborador']
            start_date_dt = pd.to_datetime(start_date)

            # Fixed: Added proper error handling
            try:
                colabs_passado = df_colaborador[
                    pd.to_datetime(df_colaborador['data_admissao']) < start_date_dt
                ]['fk_colaborador'].tolist()
                self.logger.info(f"Found {len(colabs_passado)} employees with past admission dates")
            except Exception as e:
                self.logger.error(f"Error filtering employees by admission date: {e}")
                colabs_passado = []

            # Only query if we have employees and the date range makes sense
            if len(colabs_passado) > 0 and start_date_dt != pd.to_datetime(first_date_passado):
                try:
                    query_path = CONFIG.get('available_entities_aux', {}).get('df_calendario_passado', '')
                    if not query_path:
                        self.logger.warning("df_calendario_passado query path not found in config")
                        df_calendario_passado = pd.DataFrame()
                    else:
                        df_calendario_passado = data_manager.load_data(
                            'df_calendario_passado', 
                            query_file=query_path, 
                            start_date=first_date_passado, 
                            end_date=last_date_passado.strftime('%Y-%m-%d'), 
                            colabs=colabs_passado
                        )
                except Exception as e:
                    self.logger.error(f"Error loading df_calendario_passado: {e}")
                    df_calendario_passado = pd.DataFrame()
            else:
                df_calendario_passado = pd.DataFrame()

            # Process calendar data if available
            if len(df_calendario_passado) == 0:
                self.logger.info(f"No historical calendar data for employees: {colabs_passado}")
                reshaped_final_3 = pd.DataFrame()
                emp_pre_ger = []
                df_count = pd.DataFrame()
            else:
                try:
                    reshaped_final_3, emp_pre_ger, df_count = load_wfm_scheds(
                        df_calendario_passado,  # Your DataFrame with historical schedule data
                        df_calendario_passado['employee_id'].unique().tolist()  # List of employee IDs
                    )
                    self.logger.info("Successfully processed historical calendar data")
                except Exception as e:
                    self.logger.error(f"Error in load_wfm_scheds: {e}")
                    reshaped_final_3 = pd.DataFrame()
                    emp_pre_ger = []
                    df_count = pd.DataFrame()

            # Ausencias ferias information
            try:
                query_path = CONFIG.get('available_entities_aux', {}).get('df_ausencias_ferias', '')
                if query_path:
                    colabs_id="'" + "','".join([str(x) for x in colaborador_list]) + "'"
                    df_ausencias_ferias = data_manager.load_data(
                        'df_ausencias_ferias', 
                        query_file=query_path, 
                        colabs_id=colabs_id
                    )

                else:
                    self.logger.warning("df_ausencias_ferias query path not found")
                    df_ausencias_ferias = pd.DataFrame()
            except Exception as e:
                self.logger.error(f"Error loading ausencias_ferias: {e}")
                df_ausencias_ferias = pd.DataFrame()

            # Ciclos de 90
            try:
                if len(colaborador_90_list) > 0:
                    query_path = CONFIG.get('available_entities_aux', {}).get('df_ciclos_90', '')
                    if query_path:
                        df_ciclos_90 = data_manager.load_data(
                            'df_ciclos_90', 
                            query_file=query_path, 
                            process_id=process_id, 
                            start_date=start_date, 
                            end_date=end_date, 
                            colab90ciclo=','.join(map(str, colaborador_90_list))
                        )
                    else:
                        self.logger.warning("df_ciclos_90 query path not found")
                        df_ciclos_90 = pd.DataFrame()
                else:
                    self.logger.info("No employees with 90-day cycles")
                    df_ciclos_90 = pd.DataFrame()
            except Exception as e:
                self.logger.error(f"Error loading ciclos_90: {e}")
                df_ciclos_90 = pd.DataFrame()

            # Saving results in memory
            self.auxiliary_data['df_calendario_past'] = pd.DataFrame()
            self.auxiliary_data['df_ausencias_ferias'] = df_ausencias_ferias.copy()
            self.auxiliary_data['df_ciclos_90'] = df_ciclos_90.copy()
            self.auxiliary_data['df_calendario_passado'] = reshaped_final_3.copy()
            self.auxiliary_data['emp_pre_ger'] = emp_pre_ger
            self.auxiliary_data['df_count'] = df_count.copy()
            self.raw_data['df_calendario'] = df_calendario.copy()
            
            self.logger.info("load_calendario_info completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading calendar information: {str(e)}", exc_info=True)
            return False
    
    def load_estimativas_transformations(self) -> bool:
        """
        Convert R output_turnos function to Python.
        Process shift/schedule data and calculate shift statistics.
        
        Stores results in:
        - auxiliary_data['df_turnos']: Processed shift data
        - raw_data['df_estimativas']: Final output matrix (matrizB_og equivalent)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger = logging.getLogger(self.__class__.__name__)
            logger.info("Starting load_matrices_transformations")
            
            # Get parameters from auxiliary_data and external_data
            start_date = self.external_call_data['start_date']
            end_date = self.external_call_data['end_date']
            fk_unidade = self.auxiliary_data['unit_id']
            fk_secao = self.auxiliary_data['secao_id'] 
            fk_tipo_posto = self.auxiliary_data['current_posto_id']
            
            # Get DataFrames from existing data
            df_turnos = self.auxiliary_data['df_turnos'].copy()
            df_estrutura_wfm = self.auxiliary_data['df_estrutura_wfm'].copy()
            df_faixa_horario = self.auxiliary_data['df_faixa_horario'].copy()
            df_feriados = self.auxiliary_data['df_feriados'].copy()
            df_orcamento = self.auxiliary_data['df_orcamento'].copy()  # This is dfGranularidade equivalent
            
            # Filter df_turnos by fk_tipo_posto
            df_turnos = df_turnos[df_turnos['fk_tipo_posto'] == fk_tipo_posto].copy()
            
            # Define time columns for min/max calculations
            columns_in = ["h_tm_in", "h_seg_in", "h_ter_in", "h_qua_in", "h_qui_in", "h_sex_in", "h_sab_in", "h_dom_in", "h_fer_in"]
            columns_out = ["h_tt_out", "h_seg_out", "h_ter_out", "h_qua_out", "h_qui_out", "h_sex_out", "h_sab_out", "h_dom_out", "h_fer_out"]
            
            # Calculate MinIN1 and MaxOUT2
            df_turnos['min_in1'] = df_turnos[columns_in].min(axis=1, skipna=True)
            df_turnos['max_out2'] = df_turnos[columns_out].max(axis=1, skipna=True)
            
            # Fill missing values for h_tm_out and h_tt_in
            df_turnos['h_tm_out'] = df_turnos['h_tm_out'].fillna(df_turnos['h_tt_out'])
            df_turnos['h_tt_in'] = df_turnos['h_tt_in'].fillna(df_turnos[columns_in].min(axis=1, skipna=True))
            
            # Select relevant columns
            df_turnos = df_turnos[['emp', 'fk_tipo_posto', 'min_in1', 'h_tm_out', 'h_tt_in', 'max_out2']].copy()
            
            # Fill remaining missing values
            df_turnos['min_in1'] = df_turnos['min_in1'].fillna(df_turnos['h_tt_in'])
            df_turnos['max_out2'] = df_turnos['max_out2'].fillna(df_turnos['h_tm_out'])
            
            # Convert time columns to datetime (using 2000-01-01 as base date)
            time_cols = ['min_in1', 'h_tm_out', 'h_tt_in', 'max_out2']
            for col in time_cols:
                df_turnos[col] = pd.to_datetime('2000-01-01 ' + df_turnos[col].astype(str), format='%Y-%m-%d %H:%M:%S', errors='coerce')
            
            # Handle overnight shifts (add 24 hours if end time is before start time)
            mask_tm = df_turnos['h_tm_out'] < df_turnos['min_in1']
            df_turnos.loc[mask_tm, 'h_tm_out'] += timedelta(days=1)
            
            mask_max = df_turnos['max_out2'] < df_turnos['h_tt_in']
            df_turnos.loc[mask_max, 'max_out2'] += timedelta(days=1)
            
            # Group by fk_tipo_posto and calculate aggregated times
            df_turnos_grouped = df_turnos.groupby('fk_tipo_posto').agg({
                'min_in1': 'min',
                'h_tm_out': 'max', 
                'h_tt_in': 'min',
                'max_out2': 'max'
            }).reset_index()
            
            # Calculate MED1 and MED2
            df_turnos_grouped['med1'] = np.where(
                df_turnos_grouped['h_tm_out'] < df_turnos_grouped['h_tt_in'],
                df_turnos_grouped['h_tm_out'],
                df_turnos_grouped[['h_tm_out', 'h_tt_in']].min(axis=1)
            )
            
            df_turnos_grouped['med2'] = np.where(
                df_turnos_grouped['h_tm_out'] < df_turnos_grouped['h_tt_in'],
                df_turnos_grouped['h_tt_in'], 
                df_turnos_grouped[['h_tm_out', 'h_tt_in']].min(axis=1)
            )
            
            # Select and rename columns
            df_turnos = df_turnos_grouped[['fk_tipo_posto', 'min_in1', 'med1', 'med2', 'max_out2']].copy()
            
            # Calculate MED3
            df_turnos['med3'] = df_turnos['med1'].copy()
            df_turnos['med3'] = np.where(df_turnos['med3'] < df_turnos['med2'], df_turnos['med2'], df_turnos['med3'])
            df_turnos = df_turnos.drop('med2', axis=1)
            
            # Fill missing values
            df_turnos['med3'] = df_turnos['med3'].fillna(df_turnos['med1'])
            df_turnos['max_out2'] = df_turnos['max_out2'].fillna(df_turnos['med3'])
            df_turnos['med1'] = df_turnos['med1'].fillna(df_turnos['min_in1'])
            df_turnos['med3'] = df_turnos['med3'].fillna(df_turnos['max_out2'])
            
            # Merge with estrutura_wfm
            df_estrutura_wfm_filtered = df_estrutura_wfm[df_estrutura_wfm['fk_tipo_posto'] == fk_tipo_posto].copy()
            df_turnos = pd.merge(df_estrutura_wfm_filtered, df_turnos, on='fk_tipo_posto', how='left')
            
            # Create date sequence
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            df_data = pd.DataFrame({'data': date_range})
            df_data['wd'] = df_data['data'].dt.day_name().str.lower()
            
            # Add unit information
            df_unidade = pd.DataFrame({'fk_unidade': [fk_unidade]})
            df_data = df_data.assign(key=1).merge(df_unidade.assign(key=1), on='key').drop('key', axis=1)
            
            # Process holidays
            df_feriados_filtered = df_feriados[df_feriados['fk_unidade'] == fk_unidade].copy()
            
            if len(df_feriados_filtered) > 0:
                # df_feriados_filtered['data'] = pd.to_datetime(df_feriados_filtered['data'])

                if len(df_feriados_filtered) > 0:
                    # Check what columns are available
                    self.logger.info(f"df_feriados_filtered columns: {df_feriados_filtered.columns.tolist()}")
                    
                    # Check if 'data' column exists, otherwise try 'database'
                    if 'data' in df_feriados_filtered.columns:
                        df_feriados_filtered['data'] = pd.to_datetime(df_feriados_filtered['data'])
                    elif 'database' in df_feriados_filtered.columns:
                        df_feriados_filtered['data'] = pd.to_datetime(df_feriados_filtered['database'])
                    else:
                        self.logger.error(f"No date column found in df_feriados_filtered. Available columns: {df_feriados_filtered.columns.tolist()}")
                        # Set to empty DataFrame if no date column
                        df_feriados_filtered = pd.DataFrame()
                else:
                    self.logger.info("df_feriados_filtered is empty, skipping date conversion")                

                df_feriados_filtered['tipo_dia'] = 'feriado'
                
                # Filter holidays by date range
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                year = start_dt.year
                
                mask = ((df_feriados_filtered['data'] >= start_dt) & (df_feriados_filtered['data'] <= end_dt)) | \
                    (df_feriados_filtered['data'] < pd.to_datetime('2000-12-31'))
                df_feriados_filtered = df_feriados_filtered[mask].copy()
                df_feriados_filtered['data'] = df_feriados_filtered['data'].apply(lambda x: x.replace(year=year))
                
                # Merge with data
                df_data = pd.merge(df_data, df_feriados_filtered[['fk_unidade', 'data', 'tipo_dia']], 
                                on=['fk_unidade', 'data'], how='left')
                df_data['wd'] = df_data['tipo_dia'].fillna(df_data['wd'])
                df_data = df_data.drop('tipo_dia', axis=1)
            
            # Process faixa_horario
            df_faixa_horario_filtered = df_faixa_horario[df_faixa_horario['fk_secao'] == fk_secao].copy()
            
            # Expand date ranges in faixa_horario
            expanded_rows = []
            for _, row in df_faixa_horario_filtered.iterrows():
                date_range_fh = pd.date_range(start=row['data_ini'], end=row['data_fim'], freq='D')
                for date in date_range_fh:
                    new_row = row.copy()
                    new_row['data'] = date
                    expanded_rows.append(new_row)
            
            if expanded_rows:
                df_faixa_horario_expanded = pd.DataFrame(expanded_rows)
                
                # Reshape from wide to long format for time columns
                time_columns = ["aber_seg", "fech_seg", "aber_ter", "fech_ter", "aber_qua", "fech_qua", 
                            "aber_qui", "fech_qui", "aber_sex", "fech_sex", "aber_sab", "fech_sab", 
                            "aber_dom", "fech_dom", "aber_fer", "fech_fer"]
                
                df_faixa_long = pd.melt(df_faixa_horario_expanded, 
                                    id_vars=['fk_secao', 'data', 'data_ini', 'data_fim'],
                                    value_vars=time_columns,
                                    var_name='wd_ab', value_name='value')
                
                # Split wd_ab into action (aber/fech) and weekday
                df_faixa_long[['a_f', 'wd']] = df_faixa_long['wd_ab'].str.split('_', expand=True)
                
                # Pivot back to get aber and fech columns
                df_faixa_wide = df_faixa_long.pivot_table(
                    index=['fk_secao', 'data', 'wd'], 
                    columns='a_f', 
                    values='value', 
                    aggfunc='first'
                ).reset_index()
                
                # Clean column names
                df_faixa_wide.columns.name = None
                
                # Convert weekday names and match with actual dates
                df_faixa_wide['wd'] = df_faixa_wide['wd'].str.replace('sab', 'sáb')
                df_faixa_wide['wd_date'] = df_faixa_wide['data'].dt.day_name().str.lower()
                df_faixa_wide['wd_date'] = df_faixa_wide['wd_date'].str.replace('saturday', 'sáb')
                
                # Filter matching weekdays
                df_faixa_horario_final = df_faixa_wide[df_faixa_wide['wd'] == df_faixa_wide['wd_date']].copy()
                
                # Convert time columns to datetime
                df_faixa_horario_final['aber'] = pd.to_datetime(df_faixa_horario_final['aber'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                df_faixa_horario_final['fech'] = pd.to_datetime(df_faixa_horario_final['fech'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                
                df_faixa_horario_final = df_faixa_horario_final[['fk_secao', 'data', 'aber', 'fech']]
            else:
                df_faixa_horario_final = pd.DataFrame(columns=['fk_secao', 'data', 'aber', 'fech'])
            
            # Merge all data together
            df_turnos = pd.merge(df_turnos, df_data, on=['fk_unidade'], how='left')
            df_turnos = pd.merge(df_turnos, df_faixa_horario_final, on=['fk_secao', 'data'], how='left')
            
            # Fill missing times with faixa_horario values
            df_turnos['max_out2'] = df_turnos['max_out2'].fillna(df_turnos['fech'])
            df_turnos['min_in1'] = df_turnos['min_in1'].fillna(df_turnos['aber'])
            
            # Calculate middle time
            df_turnos['middle_time'] = pd.to_datetime(
                (df_turnos['min_in1'].astype('int64') + df_turnos['max_out2'].astype('int64')) / 2,
                unit='ns'
            )
            
            # Round to nearest hour
            df_turnos['hour'] = df_turnos['middle_time'].dt.hour
            df_turnos['middle_time'] = pd.to_datetime('2000-01-01 ' + df_turnos['hour'].astype(str) + ':00:00')
            
            # Select final columns and rename
            df_turnos = df_turnos[['fk_unidade', 'unidade', 'fk_secao', 'secao', 'fk_tipo_posto', 'tipo_posto',
                                'min_in1', 'med1', 'med3', 'max_out2', 'middle_time', 'aber', 'fech', 'data']].copy()
            
            # Update med1 and med3 with middle_time
            df_turnos['med1'] = df_turnos['middle_time']
            df_turnos['med3'] = df_turnos['middle_time']
            df_turnos = df_turnos.drop('middle_time', axis=1)
            
            # Rename columns to match R output
            df_turnos.columns = ["fk_unidade", "unidade", "fk_secao", "secao", "fk_tipo_posto", "tipo_posto", 
                                "m_ini", "m_out", "t_ini", "t_out", "aber", "fech", "data"]
            
            # Reshape to long format for turnos
            df_turnos_long1 = pd.melt(df_turnos, 
                                    id_vars=[col for col in df_turnos.columns if col not in ["m_ini", "t_ini"]], 
                                    value_vars=["m_ini", "t_ini"],
                                    var_name='turno', value_name='h_ini_1')
            
            df_turnos_long2 = pd.melt(df_turnos,
                                    id_vars=[col for col in df_turnos.columns if col not in ["m_out", "t_out"]],
                                    value_vars=["m_out", "t_out"], 
                                    var_name='turno2', value_name='h_out_1')
            
            # Map turno names
            df_turnos_long1['turno'] = df_turnos_long1['turno'].map({"m_ini": "m", "t_ini": "t"})
            df_turnos_long2['turno2'] = df_turnos_long2['turno2'].map({"m_out": "m", "t_out": "t"})
            
            # Merge the two long formats
            common_cols = [col for col in df_turnos_long1.columns 
                        if col in df_turnos_long2.columns 
                        and col not in ['turno', 'h_ini_1', 'turno2', 'h_out_1']]

            df_turnos_final = pd.merge(df_turnos_long1, df_turnos_long2, on=common_cols, how='inner')
            
            # Filter matching turnos
            df_turnos_final = df_turnos_final[df_turnos_final['turno'] == df_turnos_final['turno2']].copy()
            df_turnos_final = df_turnos_final.drop('turno2', axis=1)
            
            # Filter out where start and end times are equal
            df_turnos_final = df_turnos_final[df_turnos_final['h_ini_1'] != df_turnos_final['h_out_1']].copy()
            
            # Handle overnight shifts
            mask_overnight = df_turnos_final['h_ini_1'] > df_turnos_final['h_out_1']
            df_turnos_final.loc[mask_overnight, 'h_out_1'] += timedelta(days=1)
            
            # Update fech and aber based on turno
            df_turnos_final.loc[df_turnos_final['turno'] == 'm', 'fech'] = df_turnos_final.loc[df_turnos_final['turno'] == 'm', 'h_out_1']
            df_turnos_final.loc[df_turnos_final['turno'] == 't', 'aber'] = df_turnos_final.loc[df_turnos_final['turno'] == 't', 'h_ini_1']
            
            # Adjust times based on aber/fech constraints
            mask_m = df_turnos_final['turno'] == 'm'
            df_turnos_final.loc[mask_m, 'h_ini_1'] = df_turnos_final.loc[mask_m, ['h_ini_1', 'aber']].min(axis=1)
            
            mask_t = df_turnos_final['turno'] == 't'
            df_turnos_final.loc[mask_t, 'h_out_1'] = df_turnos_final.loc[mask_t, ['h_out_1', 'fech']].max(axis=1)
            
            # Process granularity data (df_orcamento equivalent)
            # TODO: shouldnt it be from a query
            df_granularidade = self.auxiliary_data.get('df_granularidade', pd.DataFrame())
            #df_granularidade = df_orcamento[['fk_unidade', 'unidade', 'fk_secao', 'secao', 'fk_tipo_posto', 'tipo_posto', 
            #                                'data', 'hora_ini', 'pessoas_min', 'pessoas_estimado', 'pessoas_final']].copy()
            
            # Select relevant columns from df_turnos_final
            df_turnos_processing = df_turnos_final[['fk_tipo_posto', 'h_ini_1', 'h_out_1', 'turno', 'data']].copy()
            df_turnos_processing['fk_posto_turno'] = df_turnos_processing['fk_tipo_posto'].astype(str) + '_' + df_turnos_processing['turno']
            
            # Convert dates to proper format
            df_granularidade['data'] = pd.to_datetime(df_granularidade['data'])
            df_turnos_processing['data'] = pd.to_datetime(df_turnos_processing['data'])
            df_granularidade['hora_ini'] = pd.to_datetime(df_granularidade['hora_ini'])
            df_turnos_processing['h_ini_1'] = pd.to_datetime(df_turnos_processing['h_ini_1'])
            df_turnos_processing['h_out_1'] = pd.to_datetime(df_turnos_processing['h_out_1'])
            
            # Filter by fk_tipo_posto
            df_turnos_processing = df_turnos_processing[df_turnos_processing['fk_tipo_posto'] == fk_tipo_posto].copy()
            
            # Handle case where no turnos exist
            if len(df_turnos_processing) == 0:
                min_time = df_granularidade['hora_ini'].min()
                max_time = df_granularidade['hora_ini'].max()
                
                # Calculate middle time
                middle_seconds = (min_time.hour * 3600 + min_time.minute * 60 + 
                                max_time.hour * 3600 + max_time.minute * 60) / 2
                middle_hour = int(middle_seconds // 3600)
                middle_time = pd.to_datetime(f'2000-01-01 {middle_hour:02d}:00:00')
                
                # Create default turnos
                new_rows = [
                    {
                        'fk_tipo_posto': fk_tipo_posto,
                        'h_ini_1': min_time,
                        'h_out_1': middle_time,
                        'turno': 'm',
                        'data': None,
                        'fk_posto_turno': f'{fk_tipo_posto}_m'
                    },
                    {
                        'fk_tipo_posto': fk_tipo_posto,
                        'h_ini_1': middle_time,
                        'h_out_1': max_time,
                        'turno': 't', 
                        'data': None,
                        'fk_posto_turno': f'{fk_tipo_posto}_t'
                    }
                ]
                df_turnos_processing = pd.DataFrame(new_rows)
            
            # Filter granularity data
            df_granularidade = df_granularidade[df_granularidade['fk_tipo_posto'] == fk_tipo_posto].copy()
            
            # Process each unique turno
            output_final = pd.DataFrame()
            
            for i, fk_posto_turno in enumerate(df_turnos_processing['fk_posto_turno'].unique()):
                logger.info(f"Processing turno {i+1}: {fk_posto_turno}")
                
                df_turnos_f = df_turnos_processing[df_turnos_processing['fk_posto_turno'] == fk_posto_turno].copy()
                fk_posto = df_turnos_f['fk_tipo_posto'].iloc[0]
                turno = df_turnos_f['turno'].iloc[0]
                
                # Filter granularity data for this posto
                df_granularidade_f = df_granularidade[df_granularidade['fk_tipo_posto'] == fk_posto].copy()
                
                # Merge with turno data
                df_granularidade_f = pd.merge(df_granularidade_f, df_turnos_f, 
                                            on=['fk_tipo_posto', 'data'], how='inner')
                
                # Filter by time range
                time_mask = (df_granularidade_f['hora_ini'] >= df_granularidade_f['h_ini_1']) & \
                        (df_granularidade_f['hora_ini'] < df_granularidade_f['h_out_1'])
                df_granularidade_f = df_granularidade_f[time_mask].copy()
                
                df_granularidade_f = df_granularidade_f.sort_values(['data', 'hora_ini']).drop_duplicates()
                df_granularidade_f['pessoas_final'] = pd.to_numeric(df_granularidade_f['pessoas_final'], errors='coerce')
                
                # Calculate statistics
                if len(df_granularidade_f) == 0:
                    output = pd.DataFrame({
                        'data': [],
                        'media_turno': [],
                        'max_turno': [],
                        'min_turno': [],
                        'sd_turno': []
                    })
                else:
                    output = df_granularidade_f.groupby('data').agg({
                        'pessoas_final': [
                            ('media_turno', 'mean'),
                            ('max_turno', lambda x: calcular_max(x.tolist())),
                            ('min_turno', 'min'),
                            ('sd_turno', 'std')
                        ]
                    }).reset_index()
                    
                    # Flatten column names
                    output.columns = ['data', 'media_turno', 'max_turno', 'min_turno', 'sd_turno']
                
                # Create complete date range
                date_range_complete = pd.date_range(start=start_date, end=end_date, freq='D')
                df_data_complete = pd.DataFrame({'data': date_range_complete})
                
                # Merge with output
                output = pd.merge(df_data_complete, output, on='data', how='left')
                output = output.fillna(0)
                
                output['turno'] = turno
                output['fk_tipo_posto'] = fk_posto
                
                output_final = pd.concat([output_final, output], ignore_index=True)
                
                logger.info(f"Completed processing turno {fk_posto_turno}")
            
            # Final processing
            if len(output_final) > 0:
                output_final['data_turno'] = output_final['data'].astype(str) + '_' + output_final['turno']
            
            output_final = output_final.fillna(0)
            output_final = output_final.drop_duplicates()
            output_final['fk_tipo_posto'] = fk_tipo_posto
            
            # Remove duplicates based on key columns
            output_final = output_final.drop_duplicates(['fk_tipo_posto', 'data', 'data_turno', 'turno'])
            
            # Convert numeric columns
            numeric_cols = ['max_turno', 'min_turno', 'media_turno', 'sd_turno']
            for col in numeric_cols:
                output_final[col] = pd.to_numeric(output_final[col], errors='coerce')
            
            # Store results in appropriate class attributes
            # Store processed turnos data in auxiliary_data
            self.auxiliary_data['df_turnos'] = df_turnos_processing.copy()
            
            # Store final output matrix (matrizB_og equivalent) in raw_data
            self.raw_data['df_estimativas'] = output_final.copy()
            
            logger.info("load_matrices_transformations completed successfully")
            logger.info(f"Stored df_turnos with shape: {df_turnos_processing.shape}")
            logger.info(f"Stored df_estimativas (matrizB_og) with shape: {output_final.shape}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in load_matrices_transformations: {str(e)}", exc_info=True)
            return False

    def load_colaborador_transformations(self) -> bool:
        """
        Convert R loadMA_BD function to Python.
        Process employee matrix data with contract types, holidays, and labor calculations.
        
        Uses data from:
        - raw_data['df_colaborador']: Employee data (matrizA_og equivalent)
        - auxiliary_data['params_lq']: LQ parameters
        - auxiliary_data['df_festivos']: Holiday data
        - auxiliary_data['colabs_id_list']: Employee IDs list
        
        Stores results in:
        - raw_data['df_colaborador']: Updated processed employee matrix
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger = get_logger(PROJECT_NAME)
            logger.info("Starting load_ma_bd processing")
            
            # Get parameters from auxiliary_data and external_data
            colabs_id = self.auxiliary_data['colabs_id_list']
            start_date = self.external_call_data['start_date']
            end_date = self.external_call_data['end_date']
            unit_id = self.auxiliary_data['unit_id']
            
            # Get DataFrames from existing data
            matriz_ma = self.raw_data['df_colaborador'].copy()
            params_lq = self.auxiliary_data['params_lq'].copy()
            matriz_festivos = self.auxiliary_data['df_festivos'].copy()
            
            # Global variables that would be passed from external context
            # TODO: These should be configured in your config or passed as parameters
            convenio_bd = 'ALCAMPO'  # You may need to set this appropriately
            wfm_user = self.external_call_data.get('wfm_user', 'WFM')
            wfm_proc_id = self.external_call_data.get('wfm_proc_id', 0)
            path_ficheiros_global = ''  # Configure as needed
            
            # Params mapping from db values to what the algorithm needs
            params_lq_aux = pd.DataFrame({
                'seq_turno': ['M', 'T', 'MT', 'MMT', 'MTT', 'CICLO'],
                'bd_values': ['SQ_TURNO_M', 'SQ_TURNO_T', 'SQ_TURNO_MT', 'SQ_TURNO_MMT', 'SQ_TURNO_MTT', 'CICLO']
            })
            
            # Process params_lq if available
            if len(params_lq) == 0:
                logger.error('No params stored in database')
                return False
            
            # Merge params_lq with auxiliary mapping
            params_lq = pd.merge(params_lq, params_lq_aux, 
                            left_on='sys_p_name', right_on='bd_values', how='left')
            params_lq = params_lq[['seq_turno', 'numbervalue']].copy()
            params_lq.columns = ['seq_turno', 'lq']
            
            # Calculate number of holidays
            if len(matriz_festivos) == 0:
                logger.warning('No festivos stored in database')
                nr_festivos = 0
            else:
                # Filter festivos that are not Sundays (weekday != 0 in pandas)
                matriz_festivos['data'] = pd.to_datetime(matriz_festivos['data'])
                non_sunday_festivos = matriz_festivos[matriz_festivos['data'].dt.weekday != 6]  # Sunday is 6 in pandas
                nr_festivos = len(non_sunday_festivos['data'].unique())
            
            # Create day sequence from start_date to end of year
            start_dt = pd.to_datetime(start_date)
            end_of_year = pd.to_datetime(f"{start_dt.year}-12-31")
            day_seq = pd.date_range(start=start_dt, end=end_of_year, freq='D')
            
            df1 = pd.DataFrame({
                'day_seq': day_seq,
                'wd': day_seq.weekday + 1  # Convert to 1-7 where 1=Monday, 7=Sunday
            })
            
            # Params for contract types
            params_contrato = pd.DataFrame({
                'min': [2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 6],
                'max': [2, 3, 4, 3, 4, 5, 6, 4, 5, 6, 5, 6, 6],
                'tipo_contrato': [2, 3, 4, 3, 4, 5, 4, 4, 5, 6, 5, 6, 6]
            })
            
            # Check if matriz_ma is empty
            if len(matriz_ma) == 0:
                logger.error('None of the colabs ids are present at CORE_ALGORITHM_VARIABLES.')
                return False
            
            # Check for missing collaborators
            unique_colabs = matriz_ma['fk_colaborador'].unique()
            if not all(colab in unique_colabs for colab in colabs_id):
                missing_colabs = [colab for colab in colabs_id if colab not in unique_colabs]
                logger.warning(f'Colabs {missing_colabs} not present in CORE_ALGORITHM_VARIABLES, proceeding...')
            
            # Rename columns to ensure compatibility
            expected_columns = [
                "fk_colaborador", "unidade", "secao", "posto", "convenio", "nome", "matricula",
                "min_dia_trab", "max_dia_trab", "tipo_turno", "seq_turno", "t_total", "l_total",
                "dyf_max_t", "lqs", "q", "c2d", "c3d", "cxx", "semana_1", "out", "ciclo", 
                "data_admissao", "data_demissao"
            ]
            
            # Map current columns to expected columns if they differ
            if list(matriz_ma.columns) != expected_columns:
                # Create a mapping - you may need to adjust this based on your actual column names
                column_mapping = {
                    'fk_colaborador': 'fk_colaborador',
                    'loja': 'unidade', 
                    'secao': 'secao',
                    'puesto': 'posto',
                    'convenio': 'convenio',
                    'nome': 'nome',
                    'emp': 'matricula',
                    'min_dias_trabalhados': 'min_dia_trab',
                    'max_dias_trabalhados': 'max_dia_trab',
                    'tipo_de_turno': 'tipo_turno',
                    'seq_turno': 'seq_turno',
                    't_total': 't_total',
                    'l_total': 'l_total',
                    'dyf_max_t': 'dyf_max_t',
                    'lq': 'lqs',
                    'q': 'q',
                    'fds_cal_2d': 'c2d',
                    'fds_cal_3d': 'c3d',
                    'd_cal_xx': 'cxx',
                    'semana_1': 'semana_1',
                    'out': 'out',
                    'ciclo': 'ciclo',
                    'data_admissao': 'data_admissao',
                    'data_demissao': 'data_demissao'
                }
                
                # Rename columns that exist in the mapping
                for old_col, new_col in column_mapping.items():
                    if old_col in matriz_ma.columns:
                        matriz_ma = matriz_ma.rename(columns={old_col: new_col})
            
            # Transform data types
            matriz_ma['convenio'] = matriz_ma['convenio'].str.upper()
            matriz_ma['min_dia_trab'] = pd.to_numeric(matriz_ma['min_dia_trab'], errors='coerce')
            matriz_ma['max_dia_trab'] = pd.to_numeric(matriz_ma['max_dia_trab'], errors='coerce')
            matriz_ma['dyf_max_t'] = pd.to_numeric(matriz_ma['dyf_max_t'], errors='coerce')
            matriz_ma['c2d'] = pd.to_numeric(matriz_ma['c2d'], errors='coerce')
            matriz_ma['c3d'] = pd.to_numeric(matriz_ma['c3d'], errors='coerce')
            matriz_ma['cxx'] = pd.to_numeric(matriz_ma['cxx'], errors='coerce')
            matriz_ma['lqs'] = pd.to_numeric(matriz_ma['lqs'], errors='coerce')
            
            # Convert dates
            matriz_ma['data_admissao'] = pd.to_datetime(matriz_ma['data_admissao'], errors='coerce')
            matriz_ma['data_demissao'] = pd.to_datetime(matriz_ma['data_demissao'], errors='coerce')
            
            # Add EMP column (padded zeros) - implement pad_zeros equivalent
            matriz_ma['emp'] = matriz_ma['matricula'].astype(str).str.zfill(10)  # Adjust padding as needed
            
            # Fill missing min/max working days
            matriz_ma['min_dia_trab'] = matriz_ma['min_dia_trab'].fillna(matriz_ma['max_dia_trab'])
            matriz_ma['max_dia_trab'] = matriz_ma['max_dia_trab'].fillna(matriz_ma['min_dia_trab'])
            
            # Merge with params_lq
            matriz_ma = pd.merge(matriz_ma, params_lq, on='seq_turno', how='left')
            
            # Merge with params_contrato
            matriz_ma = pd.merge(matriz_ma, params_contrato, 
                            left_on=['min_dia_trab', 'max_dia_trab'], 
                            right_on=['min', 'max'], how='left')
            
            # Fill missing values (except date columns)
            date_columns = ['data_admissao', 'data_demissao']
            non_date_columns = [col for col in matriz_ma.columns if col not in date_columns]
            matriz_ma[non_date_columns] = matriz_ma[non_date_columns].fillna(0)
            
            # Validate seq_turno
            if (matriz_ma['seq_turno'] == 0).any() or matriz_ma['seq_turno'].isna().any():
                logger.error("seq_turno=0 or null - columna SEQ_TURNO mal parametrizada")
                return False
            
            # Validate tipo_contrato
            if (matriz_ma['tipo_contrato'] == 0).any() or matriz_ma['tipo_contrato'].isna().any():
                logger.error("contrato=0 or null - columna TIPO_CONTRATO mal parametrizada")
                return False
            
            # Calculate numFerDom and ferFechados
            num_sundays = len(df1[df1['wd'] == 7])  # Sunday is 7
            num_fer_dom = nr_festivos + num_sundays
            
            # Calculate closed holidays (tipo == 3 and not Sunday)
            if len(matriz_festivos) > 0:
                closed_festivos = matriz_festivos[
                    (matriz_festivos['tipo'] == 3) & 
                    (matriz_festivos['data'].dt.weekday != 6)  # Not Sunday
                ]
                fer_fechados = len(closed_festivos['data'].unique())
            else:
                fer_fechados = 0
            
            # Update LQ based on seq_turno
            special_turnos = ['CICLO', 'MOT', 'P']
            mask_special = matriz_ma['seq_turno'].str.upper().isin(special_turnos)
            matriz_ma.loc[mask_special, 'lq'] = matriz_ma.loc[mask_special, 'lqs']
            matriz_ma = matriz_ma.drop('lqs', axis=1)
            
            # Process each collaborator
            processed_rows = []
            
            for colab in matriz_ma['fk_colaborador'].unique():
                cc = matriz_ma[matriz_ma['fk_colaborador'] == colab].copy()
                
                if len(cc) == 0:
                    continue
                    
                cc = cc.iloc[0:1].copy()  # Take first row if multiple
                
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                
                # Adjust for admission date
                if pd.notna(cc['data_admissao'].iloc[0]) and start_dt < cc['data_admissao'].iloc[0]:
                    days_from_admission = (end_dt - cc['data_admissao'].iloc[0]).days + 1
                    total_days = (end_dt - start_dt).days + 1
                    div = days_from_admission / total_days
                    
                    cc['dyf_max_t'] = np.ceil(cc['dyf_max_t'] * div)
                    cc['lq'] = np.ceil(cc['lq'] * div)
                    cc['c2d'] = np.ceil(cc['c2d'] * div)
                    cc['c3d'] = np.ceil(cc['c3d'] * div)
                
                # Apply business logic: C2D = C2D + C3D
                cc['c2d'] = cc['c2d'] + cc['c3d']
                
                tipo_contrato = cc['tipo_contrato'].iloc[0]
                convenio = cc['convenio'].iloc[0]
                
                # Process based on contract type and convention
                if tipo_contrato == 6 and convenio == convenio_bd:
                    cc['ld'] = cc['dyf_max_t']
                    cc['l_dom'] = num_fer_dom - cc['dyf_max_t'] - fer_fechados
                    cc['lq_og'] = cc['lq'].copy()
                    cc['lq'] = cc['lq'] - (cc['c2d'] + cc['c3d'])
                    cc['l_total'] = num_fer_dom + cc['lq'] + cc['c2d'] + cc['c3d']
                    
                    if cc['lq'].iloc[0] < 0:
                        cc['c3d'] = cc['c3d'] + cc['lq']
                        cc['lq'] = 0
                        logger.error(f"Empleado {cc['matricula'].iloc[0]} sin suficiente LQ para fines de semana de calidad")
                
                elif tipo_contrato in [5, 4] and convenio == convenio_bd:
                    logger.info("teste entre novo convenio")
                    cc['ld'] = cc['dyf_max_t']
                    cc['l_dom'] = num_fer_dom - cc['dyf_max_t'] - fer_fechados
                    cc['lq_og'] = 0
                    cc['lq'] = 0
                    cc['l_total'] = num_sundays * (7 - tipo_contrato)
                
                elif tipo_contrato in [3, 2] and convenio == convenio_bd:
                    if len(matriz_festivos) > 0:
                        coh = count_open_holidays(matriz_festivos, tipo_contrato)
                        cc['dyf_max_t'] = 0
                        cc['q'] = 0
                        cc['lq_og'] = 0
                        cc['lq'] = 0
                        cc['c2d'] = 0
                        cc['c3d'] = 0
                        cc['cxx'] = 0
                        cc['ld'] = 0
                        cc['l_dom'] = coh[0]
                        cc['l_total'] = coh[1] - coh[0]
                    else:
                        cc['dyf_max_t'] = 0
                        cc['q'] = 0
                        cc['lq_og'] = 0
                        cc['lq'] = 0
                        cc['c2d'] = 0
                        cc['c3d'] = 0
                        cc['cxx'] = 0
                        cc['ld'] = 0
                        cc['l_dom'] = 0
                        cc['l_total'] = 0
                
                elif tipo_contrato == 6 and convenio == 'SABECO':
                    cc['ld'] = cc['dyf_max_t']
                    cc['l_dom'] = num_fer_dom - cc['dyf_max_t'] - fer_fechados
                    cc['c3d'] = 0
                    cc['lq'] = 0
                    cc['lq_og'] = 0
                    cc['l_total'] = num_fer_dom + cc['c2d']
                
                elif tipo_contrato in [5, 4] and convenio == 'SABECO':
                    cc['ld'] = cc['dyf_max_t']
                    cc['l_dom'] = num_fer_dom - cc['dyf_max_t'] - fer_fechados
                    cc['c3d'] = 0
                    cc['lq'] = 0
                    cc['lq_og'] = 0
                    cc['l_total'] = num_sundays * (7 - tipo_contrato) + 8  # 8 is hardcoded per business rule
                
                elif tipo_contrato in [3, 2] and convenio == 'SABECO':
                    coh = count_open_holidays(matriz_festivos, tipo_contrato)
                    cc['dyf_max_t'] = 0
                    cc['q'] = 0
                    cc['lq'] = 0
                    cc['lq_og'] = 0
                    cc['c2d'] = 0
                    cc['c3d'] = 0
                    cc['cxx'] = 0
                    cc['ld'] = 0
                    cc['l_dom'] = coh[0]
                    cc['l_total'] = coh[1] - coh[0]
                
                processed_rows.append(cc)

            self.logger.info(f"columnes matriz a: {matriz_ma.columns.tolist()}")
            
            # Combine all processed rows
            if processed_rows:
                matriz_ma_final = pd.concat(processed_rows, ignore_index=True)
            else:
                matriz_ma_final = pd.DataFrame()
                
            # Final validation - check for negative L_DOM
            if len(matriz_ma_final) > 0 and 'l_dom' in matriz_ma_final.columns and (matriz_ma_final['l_dom'] < 0).any():
                logger.error("l_dom < 0 - columna DyF_MAX_T mal parametrizada")
                return False
            
            # Store result in raw_data
            self.raw_data['df_colaborador'] = matriz_ma_final.copy()
            self.auxiliary_data['num_fer_doms'] = num_fer_dom
            
            logger.info(f"load_ma_bd completed successfully. Processed {len(matriz_ma_final)} employees.")
            return True
            
        except Exception as e:
            logger.error(f"Error in load_ma_bd: {str(e)}", exc_info=True)
            return False

    def load_calendario_transformations(self) -> bool:
        """
        Convert R loadM2_BD function to Python.
        Creates the calendar matrix (matriz2_og) by processing employee schedules,
        90-day cycles, holidays, absences, and days off.
        
        Uses data from:
        - raw_data['df_colaborador']: Employee data (matrizMA equivalent)
        - auxiliary_data['df_ciclos_90']: 90-day cycle information
        - auxiliary_data['df_ausencias_ferias']: Absences and vacation data
        - auxiliary_data['df_feriados']: Holiday data
        
        Stores results in:
        - raw_data['df_calendario']: Final calendar matrix (matriz2_og equivalent)
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger = get_logger(PROJECT_NAME)
            logger.info("Starting load_m2_bd processing")
            
            # Get parameters from auxiliary_data and external_data
            start_date = self.external_call_data['start_date']
            end_date = self.external_call_data['end_date']
            unidade = self.auxiliary_data['unit_id']
            process_id = self.external_call_data['current_process_id']
            current_year = pd.to_datetime(start_date).year
            
            # Get DataFrames from existing data
            matriz_ma = self.raw_data['df_colaborador'].copy()
            df_feriados = self.auxiliary_data['df_feriados'].copy()
            df_ciclos_90 = self.auxiliary_data['df_ciclos_90'].copy()
            df_ausencias_ferias = self.auxiliary_data['df_ausencias_ferias'].copy()
            closed_days = self.auxiliary_data.get('df_closed_days', pd.DataFrame())
            if closed_days is None:
                closed_days = pd.DataFrame()
            
            # Prepare contract type mapping
            df_tipo_contrato = matriz_ma[['matricula', 'tipo_contrato']].copy()
            df_tipo_contrato = df_tipo_contrato[df_tipo_contrato['tipo_contrato'].notna()]
            df_tipo_contrato.columns = ['emp', 'tipo_contrato']
            
            all_colab_pad = matriz_ma['emp'].tolist()
            colabs_id = self.auxiliary_data['colabs_id_list']
            
            # Pre-generated schedules - currently empty as per R code
            df_pre_ger_db = pd.DataFrame()
            
            # Create base schedule matrix
            if len(df_pre_ger_db) >= 1:
                # TODO: Implement load_pre_ger_scheds logic if needed
                reshaped_final_3, emp_pre_ger = load_pre_ger_scheds(df_pre_ger_db, all_colab_pad)
            else:
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                
                # Generate date sequence
                date_range = pd.date_range(start=start_dt, end=end_dt, freq='D')
                
                # Create the matrix structure that func_inicializa expects
                # First create lists for dates and turnos
                dates_list = []
                turnos_list = []
                
                # Add each date twice (for M and T shifts)
                for date in date_range:
                    date_str = date.strftime('%Y-%m-%d')
                    dates_list.extend([date_str, date_str])
                    turnos_list.extend(['M', 'T'])
                
                # Create the matrix with proper header structure
                # Row 0: "Dia" followed by dates
                dia_row = ['Dia'] + dates_list
                # Row 1: "TURNO" followed by shift types
                turno_row = ['TURNO'] + turnos_list
                
                # Create DataFrame with proper structure
                max_length = max(len(dia_row), len(turno_row))
                
                # Ensure both rows have the same length
                while len(dia_row) < max_length:
                    dia_row.append('')
                while len(turno_row) < max_length:
                    turno_row.append('')
                
                # Create the DataFrame
                reshaped_final_3 = pd.DataFrame([dia_row, turno_row])
                
                # Reset column names to be numeric indices
                reshaped_final_3.columns = range(len(reshaped_final_3.columns))
                
                emp_pre_ger = []
            
            # Process 90-day cycles
            matriculas_90_cycles = []
            
            # Find employees with 90-day cycles
            colab_90_ciclo = matriz_ma[matriz_ma['seq_turno'].str.upper() == 'CICLO']['fk_colaborador'].tolist()
            
            if len(colab_90_ciclo) > 0 and len(df_ciclos_90) > 0:
                cycle90_info = df_ciclos_90.copy()
                
                # Group and deduplicate 90-day cycle information
                cycle90_info = (cycle90_info.groupby(['schedule_day', 'employee_id'])
                            .first()
                            .reset_index())
                
                cycle90_info = cycle90_info[[
                    'process_id', 'employee_id', 'matricula', 'schedule_day', 'tipo_dia', 'descanso',
                    'horario_ind', 'hora_ini_1', 'hora_fim_1', 'hora_ini_2', 'hora_fim_2', 'fk_horario',
                    'nro_semana', 'dia_semana', 'minimumworkday', 'maximumworkday'
                ]].copy()
                
                # Convert day numbers from DB format to R format
                cycle90_info['dia_semana'] = cycle90_info['dia_semana'].apply(
                    lambda x: x - 6 if x == 7 else (8 if x == 8 else x + 1)
                )
                
                # Get unique collaborators with 90-day cycles
                colabs_90_cycle = cycle90_info['employee_id'].unique()
                cycle90_info['schedule_day'] = pd.to_datetime(cycle90_info['schedule_day'])
                
                # Process each collaborator with 90-day cycles
                for colab in colabs_90_cycle:
                    df_cycle90_info_filtered = cycle90_info[
                        (cycle90_info['employee_id'] == colab) &
                        (cycle90_info['schedule_day'] >= pd.to_datetime(start_date)) &
                        (cycle90_info['schedule_day'] <= pd.to_datetime(end_date))
                    ].copy()
                    
                    if len(df_cycle90_info_filtered) == 0:
                        continue
                    
                    matricula = df_cycle90_info_filtered['matricula'].iloc[0]
                    matriculas_90_cycles.append(matricula)
                    
                    lim_sup_manha, lim_inf_tarde = get_limit_mt(matricula, self.raw_data['df_colaborador'])
                    
                    # Add new row for this employee
                    row_filling = ['-'] * (reshaped_final_3.shape[1] - 1)
                    new_row = [matricula] + row_filling
                    
                    # Add the new row to the DataFrame
                    new_row_df = pd.DataFrame([new_row], columns=reshaped_final_3.columns)
                    reshaped_final_3 = pd.concat([reshaped_final_3, new_row_df], ignore_index=True)
                    
                    # Find row index for this employee
                    reshaped_row_index = reshaped_final_3.index[
                        reshaped_final_3.apply(lambda row: matricula in row.values, axis=1)
                    ].tolist()[-1]
                    
                    # Process each day for this employee
                    for _, cycle_row in df_cycle90_info_filtered.iterrows():
                        day = cycle_row['schedule_day'].strftime('%Y-%m-%d')
                        
                        # Find column index for this day
                        reshaped_col_index = None
                        for col_idx, col_data in reshaped_final_3.items():
                            if day in col_data.values:
                                reshaped_col_index = col_idx
                                break
                        
                        if reshaped_col_index is not None:
                            reshaped_final_3 = assign_90_cycles(
                                reshaped_final_3, df_cycle90_info_filtered, colab, df_feriados,
                                lim_sup_manha, lim_inf_tarde, day, reshaped_col_index, 
                                reshaped_row_index, matricula
                            )
            
            # Process employees not in pre-generated or 90-cycle schedules
            not_in_pre_ger = [emp for emp in all_colab_pad if emp not in emp_pre_ger]
            not_pre_ger_nor90cycle = [emp for emp in not_in_pre_ger if emp not in matriculas_90_cycles]
            
            # Filter algorithm variables for remaining employees
            df_alg_variables_filtered = matriz_ma[
                matriz_ma['emp'].isin(not_pre_ger_nor90cycle)
            ][['emp', 'seq_turno', 'semana_1']].copy()
            
            # Create MT/MTT cycles for remaining employees
            if len(df_alg_variables_filtered) > 0:
                reshaped_final_3 = create_mt_mtt_cycles(df_alg_variables_filtered, reshaped_final_3)
            
            # Create M0/0T patterns
            reshaped_final_3 = create_m0_0t(reshaped_final_3)
            
            # Process absences and holidays
            if len(df_ausencias_ferias) > 0 and 'data_ini' in df_ausencias_ferias.columns:
                ausencias_total = df_ausencias_ferias[
                    (df_ausencias_ferias['data_ini'] >= pd.to_datetime(f"{current_year}-01-01")) &
                    (df_ausencias_ferias['data_ini'] <= pd.to_datetime(f"{current_year}-12-31"))
                ].copy()
            else:
                ausencias_total = pd.DataFrame()
            
            # Filter holidays for current year
            if 'database' in df_feriados.columns:
                df_feriados = df_feriados.rename(columns={'database': 'data'})
            
            df_feriados['data'] = pd.to_datetime(df_feriados['data'])  # Convert to datetime first
            df_feriados_filtered = df_feriados[
                (df_feriados['data'] >= pd.to_datetime(f"{current_year}-01-01")) &
                (df_feriados['data'] <= pd.to_datetime(f"{current_year}-12-31"))
            ].copy()
            
            # Insert holidays and absences
            if len(ausencias_total) > 0 and not ausencias_total.empty:
                reshaped_final_3 = insert_holidays_absences(all_colab_pad, ausencias_total, reshaped_final_3)
            
            if len(df_feriados_filtered) > 0:
                reshaped_final_3 = insert_feriados(df_feriados_filtered, reshaped_final_3)
            else:
                # Add TIPO_DIA row when no holidays
                new_row = ['-'] * reshaped_final_3.shape[1]
                new_row[0] = "TIPO_DIA"
                
                upper_bind = reshaped_final_3.iloc[:2].copy()  # Take first 2 rows (Dia and TURNO)
                lower_bind = reshaped_final_3.iloc[2:].copy()  # Take remaining rows
                
                new_row_df = pd.DataFrame([new_row], columns=reshaped_final_3.columns)
                reshaped_final_3 = pd.concat([upper_bind, new_row_df, lower_bind], ignore_index=True)
            
            # Insert closed days if available
            if closed_days is not None and len(closed_days) > 0:
                reshaped_final_3 = insert_closed_days(closed_days, reshaped_final_3)
            
            # Assign empty days for contract types
            if len(df_tipo_contrato) > 0:
                reshaped_final_3 = assign_empty_days(
                    df_tipo_contrato, reshaped_final_3, not_in_pre_ger, df_feriados_filtered
                )
            
            # Final insertion of holidays and absences
            if len(ausencias_total) > 0 and not ausencias_total.empty:
                reshaped_final_3 = insert_holidays_absences(all_colab_pad, ausencias_total, reshaped_final_3)
            
            # Process days off
            df_days_off_filtered = pd.DataFrame()  # TODO: Implement get_days_off equivalent if needed
            if len(df_days_off_filtered) > 0:
                reshaped_final_3 = assign_days_off(reshaped_final_3, df_days_off_filtered)
            
            # Store result in raw_data
            self.raw_data['df_calendario'] = reshaped_final_3.copy()
            
            logger.info(f"load_m2_bd completed successfully. Created calendar matrix with shape: {reshaped_final_3.shape}")
            logger.info(f"First few rows of created matrix:")
            logger.info(f"{reshaped_final_3.head()}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error in load_m2_bd: {str(e)}", exc_info=True)

    def validate_matrices_loading(self) -> bool:
        """
        Validate that the required data is present, conforming, and valid.
        
        Returns:
            True if validation passes, False otherwise
        """
        return True

    def func_inicializa(self, start_date: str, end_date: str, fer, closed_days) -> bool:
        """
        Python translation of R funcInicializa function.
        Initializes matrices and performs data transformations.
        
        Args:
            start_date: Start date string
            end_date: End date string  
            fer: Holiday data
            closed_days: Closed days data
            semanas_restantes: Remaining weeks
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            import pandas as pd
            import numpy as np
            from datetime import datetime
            
            # Get matrices from existing data
            matriz2_og = self.raw_data['df_calendario'].copy()
            matrizB_og = self.raw_data['df_estimativas'].copy() 
            matrizA_og = self.raw_data['df_colaborador'].copy()

            # TODO: Remove this debug code
            # Debug: Check the structure of matriz2_og
            self.logger.info(f"matriz2_og shape: {matriz2_og.shape}")
            self.logger.info(f"matriz2_og first few rows:\n{matriz2_og.head()}")
            self.logger.info(f"matriz2_og first column unique values: {matriz2_og.iloc[:, 0].unique()}")
            
            # Check if TURNO and Dia rows exist
            turno_exists = (matriz2_og.iloc[:, 0] == 'TURNO').any()
            dia_exists = (matriz2_og.iloc[:, 0] == 'Dia').any()
            
            self.logger.info(f"TURNO row exists: {turno_exists}")
            self.logger.info(f"Dia row exists: {dia_exists}")
            
            if not turno_exists or not dia_exists:
                self.logger.error("Required header rows (TURNO/Dia) not found in matriz2_og")
                return False
            
            # Find TURNO and Dia rows
            turno_row_idx = matriz2_og[matriz2_og.iloc[:, 0] == 'TURNO'].index[0]
            dia_row_idx = matriz2_og[matriz2_og.iloc[:, 0] == 'Dia'].index[0]

            # Semanas restantes calculo
            date_obj = datetime.strptime(end_date, '%Y-%m-%d')
            semana_inicial = date_obj.isocalendar().week
            semanas_restantes = 52 - semana_inicial
            
            # Get year from matrizB_og
            ano = pd.to_datetime(matrizB_og['data'].min()).year
            
            # Adjust minTurno for specific dates
            special_dates = [f'{ano}-12-23', f'{ano}-12-24', f'{ano}-12-30', f'{ano}-12-31']
            friday_dates = [f'{ano}-12-22', f'{ano}-12-29']
            
            matrizB_ini = matrizB_og.copy()
            matrizB_ini.loc[matrizB_ini['data'].isin(special_dates), 'min_turno'] = matrizB_ini['max_turno']
            mask_friday = (matrizB_ini['data'].isin(friday_dates)) & (matrizB_ini['turno'] == 'M')
            matrizB_ini.loc[mask_friday, 'min_turno'] = matrizB_ini.loc[mask_friday, 'max_turno']
            
            #CRIAR MATRIZ_2--------------------------------------------------
            
            # Reshape matriz2_og (equivalent to R melt)
            # Find TURNO and Dia rows
            turno_row_idx = matriz2_og[matriz2_og.iloc[:, 0] == 'TURNO'].index[0]
            dia_row_idx = matriz2_og[matriz2_og.iloc[:, 0] == 'Dia'].index[0]
            
            # Create column names
            dia_values = matriz2_og.iloc[dia_row_idx, 1:].values
            turno_values = matriz2_og.iloc[turno_row_idx, 1:].values
            new_columns = [f"{dia}_{turno}" for dia, turno in zip(dia_values, turno_values)]
            new_columns.insert(0, 'DIA_TURNO')
            
            # Rename columns
            matriz2_og.columns = new_columns
            
            # Melt the dataframe
            matriz2_ini = pd.melt(matriz2_og, id_vars='DIA_TURNO', 
                                var_name='DATA', value_name='TIPO_TURNO')
            matriz2_ini.columns = ['COLABORADOR', 'DATA', 'TIPO_TURNO']
            
            # Filter by date range
            matriz2_ini['DATA'] = matriz2_ini['DATA'].astype(str)
            matriz2_ini = matriz2_ini[
                (matriz2_ini['DATA'] >= start_date) & 
                (matriz2_ini['DATA'] <= end_date)
            ]
            
            # Filter out unwanted rows
            unwanted_colaboradors = ['Dia', 'maxTurno', 'mediaTurno', 'minTurno', 'sdTurno', 'TURNO']
            matriz2 = matriz2_ini[~matriz2_ini['COLABORADOR'].isin(unwanted_colaboradors)].copy()
            
            # Clean DATA column (remove everything after underscore)
            matriz2['DATA'] = matriz2['DATA'].str.replace(r'_.*$', '', regex=True)
            
            # Add HORARIO column
            matriz2['HORARIO'] = np.where(
                matriz2['TIPO_TURNO'].isin(['M', 'T', 'MoT', 'P']), 'H', matriz2['TIPO_TURNO']
            )
            
            # Get unique shift types
            tipos_de_turno = matriz2['TIPO_TURNO'].unique().tolist()
            
            # Process MoT and P shifts if they exist
            if 'MoT' in tipos_de_turno:
                matriz2 = func_turnos(matriz2, 'MoT')
            if 'P' in tipos_de_turno:
                matriz2 = func_turnos(matriz2, 'P')
            
            # Add date-related columns
            matriz2['DATA'] = pd.to_datetime(matriz2['DATA'])
            matriz2['WDAY'] = matriz2['DATA'].dt.dayofweek + 1  # Convert to 1-7 scale
            matriz2['ID'] = range(len(matriz2))
            matriz2['WW'] = matriz2['DATA'].apply(adjusted_isoweek)
            matriz2['WD'] = matriz2['DATA'].dt.day_name().str[:3]
            
            # Calculate DIA_TIPO
            def calculate_dia_tipo(group):
                has_feriado = (group['TIPO_TURNO'] == 'F').any()
                is_sunday = group['WD'].iloc[0] == 'Sun'
                not_feriado_horario = group['HORARIO'] != 'F'
                
                if (has_feriado or is_sunday) and not_feriado_horario.any():
                    return 'domYf'
                else:
                    return group['WD'].iloc[0]
            
            matriz2['DIA_TIPO'] = matriz2.groupby('DATA').apply(calculate_dia_tipo).reset_index(drop=True)
            
            # Merge with employee admission/dismissal dates
            emp_dates = matrizA_og[['emp', 'data_admissao', 'data_demissao']].copy()
            emp_dates = pd.concat([
                emp_dates,
                pd.DataFrame({'emp': ['TIPO_DIA'], 'data_admissao': [pd.NaT], 'data_demissao': [pd.NaT]})
            ])
            
            matriz2 = matriz2.merge(emp_dates, left_on='COLABORADOR', right_on='emp', how='left')
            
            # Filter by dismissal date and adjust HORARIO based on admission date
            matriz2 = matriz2[
                matriz2['DATA'] <= matriz2['data_demissao'].fillna(pd.Timestamp('2100-01-01'))
            ]
            
            def adjust_horario(row):
                admission_date = row['data_admissao'] if pd.notna(row['data_admissao']) else pd.Timestamp('1900-01-01')
                if row['DATA'] >= admission_date:
                    return row['HORARIO']
                elif row['DATA'] < admission_date and row['DIA_TIPO'] == 'domYf':
                    return 'L_'
                else:
                    return 'NL'
            
            matriz2['HORARIO'] = matriz2.apply(adjust_horario, axis=1)
            
            #CRIAR MATRIZ_A--------------------------------------------------
            
            # Extract festivos (holidays)
            dom_e_fes = matriz2[
                (matriz2['HORARIO'] == 'F') & 
                (matriz2['COLABORADOR'] == 'TIPO_DIA')
            ]['DATA'].unique()
            
            # Filter matrizA_og
            matrizA_og = matrizA_og[matrizA_og['matricula'] != ''].copy()
            
            # Initialize counting dataframes
            df_merge_count_dom_fes = []
            df_merge_count_fes = []
            df_merge_count_holidays = []
            
            # Get convenio from external data or config
            convenio_bd = self.external_call_data.get('convenio_bd', 'BD')  # You may need to adjust this
            
            # Process each employee
            for matricula in matrizA_og['matricula'].unique():
                if matricula == '':
                    continue
                    
                matriz_temp = matriz2[matriz2['COLABORADOR'] == matricula].copy()
                #matriz_temp['DATA'] = matriz_temp['DATA'].astype(str)
                tipo_contrato = matrizA_og[matrizA_og['matricula'] == matricula]['tipo_contrato'].iloc[0]
                
                if convenio_bd == 'BD':  # Assuming BD convention
                    if tipo_contrato in [4, 5]:
                        # Count occurrences for 4/5 day contracts
                        count_occurrences = len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            (matriz_temp['WDAY'] == 1)
                        ])
                        
                        # Subtract closed days that fell on holidays
                        fer_tipo3 = fer[fer['tipo'] == 3]['data'] if 'tipo' in fer.columns else []
                        count_occurrences_extra2 = len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            matriz_temp['DATA'].isin(fer_tipo3) & 
                            (matriz_temp['WDAY'] == 1)
                        ])
                        count_occurrences -= count_occurrences_extra2
                        
                        # Count holidays
                        count_occurrences_fes = len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            matriz_temp['DATA'].isin(dom_e_fes)
                        ])
                        count_occurrences_fes -= len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            matriz_temp['DATA'].isin(fer_tipo3) & 
                            (matriz_temp['WDAY'] != 1)
                        ])
                        
                        count_holidays = len(matriz_temp[matriz_temp['HORARIO'] == 'V'])
                        
                    else:
                        # For other contract types
                        count_occurrences = len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            ((matriz_temp['WDAY'] == 1) | matriz_temp['DATA'].isin(dom_e_fes))
                        ])
                        
                        fer_tipo3 = fer[fer['tipo'] == 3]['data'] if 'tipo' in fer.columns else []
                        count_occurrences_extra2 = len(matriz_temp[
                            matriz_temp['HORARIO'].isin(['A', 'L', 'V', 'L_', 'L_DOM', 'DFS']) & 
                            matriz_temp['DATA'].isin(fer_tipo3)
                        ])
                        count_occurrences -= count_occurrences_extra2
                        
                        count_occurrences_fes = 0
                        count_holidays = 0
                
                else:
                    # SABECO convention
                    count_occurrences = len(matriz_temp[
                        (matriz_temp['HORARIO'] != 'NL') & 
                        matriz_temp['HORARIO'].str.contains('^L|DFS', case=False, na=False) & 
                        ((matriz_temp['WDAY'] == 1) | matriz_temp['DATA'].isin(dom_e_fes))
                    ])
                    count_occurrences_fes = 0
                    count_holidays = 0
                
                # Store results
                df_merge_count_dom_fes.append({
                    'matricula': matricula, 
                    'total_dom_fes': count_occurrences
                })
                df_merge_count_fes.append({
                    'matricula': matricula, 
                    'total_fes': count_occurrences_fes
                })
                df_merge_count_holidays.append({
                    'matricula': matricula, 
                    'total_holidays': count_holidays
                })
            
            # Convert to DataFrames and merge
            df_merge_count_dom_fes = pd.DataFrame(df_merge_count_dom_fes).drop_duplicates()
            df_merge_count_fes = pd.DataFrame(df_merge_count_fes).drop_duplicates()
            df_merge_count_holidays = pd.DataFrame(df_merge_count_holidays).drop_duplicates()
            
            matrizA_og = matrizA_og.merge(df_merge_count_dom_fes, on='matricula', how='left')
            matrizA_og = matrizA_og.merge(df_merge_count_fes, on='matricula', how='left')
            matrizA_og = matrizA_og.merge(df_merge_count_holidays, on='matricula', how='left')
            
            # Divide by 2 (as in R code)
            matrizA_og['total_dom_fes'] = matrizA_og['total_dom_fes'] / 2
            matrizA_og['total_fes'] = matrizA_og['total_fes'] / 2
            matrizA_og['total_holidays'] = matrizA_og['total_holidays'] / 2
            
            # Adjust for contract type 3
            matrizA_og.loc[matrizA_og['tipo_contrato'] == 3, 'total_dom_fes'] = 0
            matrizA_og.loc[matrizA_og['tipo_contrato'] == 3, 'dyf_max_t'] = 0
            
            # Add DESCANSOS_ATRB column
            matrizA_og['descansos_atrb'] = 0
            
            # CALCULA LIBRANÇAS ------------------------------------------------------------
            
            # Create df_CD for consecutive day calculations
            df_cd = matriz2[matriz2['COLABORADOR'].isin(matrizA_og['matricula'])].copy()
            df_cd['WD'] = df_cd['DATA'].dt.dayofweek + 1  # Convert to 1-7 scale
            df_cd = df_cd.sort_values(['COLABORADOR', 'DATA'])
            
            # Remove duplicates by keeping first occurrence per COLABORADOR/DATA
            df_cd = df_cd.groupby(['COLABORADOR', 'DATA']).first().reset_index()
            
            # Add next/previous day shift information
            df_cd = df_cd.sort_values(['COLABORADOR', 'DATA'])
            df_cd['TIPO_TURNO_NEXT'] = df_cd.groupby('COLABORADOR')['HORARIO'].shift(-1).fillna('H')
            df_cd['TIPO_TURNO_PREV'] = df_cd.groupby('COLABORADOR')['HORARIO'].shift(1).fillna('H')
            df_cd['WDAY_NEXT'] = df_cd.groupby('COLABORADOR')['WDAY'].shift(-1)
            df_cd['TIPO_TURNO_NEXT2'] = df_cd.groupby('COLABORADOR')['HORARIO'].shift(-2).fillna('H')
            df_cd['WDAY_NEXT2'] = df_cd.groupby('COLABORADOR')['WDAY'].shift(-2)
            df_cd['WDAY_PREV'] = df_cd.groupby('COLABORADOR')['WDAY'].shift(1)
            
            # Process 4/5 day contracts - count different types of rest days
            colabs_45d = matrizA_og[matrizA_og['tipo_contrato'].isin([4, 5])]['matricula'].tolist()
            
            count_ldt_45d_data = []
            for colab in colabs_45d:
                colab_data = df_cd[df_cd['COLABORADOR'] == colab].copy()
                
                # Calculate LD_at, LQ_at, LRES_at, CXX_at
                ld_at = 0
                
                # LQ_at calculation
                lq_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (colab_data['HORARIO'] != 'LD') &
                    (colab_data['HORARIO'].str.contains('^L', case=False, na=False)) &
                    (colab_data['WD'].isin([2, 3, 4, 5, 6, 7])) &
                    (~colab_data['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False)) &
                    (colab_data['TIPO_TURNO_PREV'] != 'L')
                )
                lq_at = lq_condition.sum()
                
                # LRES_at calculation  
                lres_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (colab_data['HORARIO'].str.contains('^L', case=False, na=False)) &
                    (colab_data['WD'].isin([2, 3, 4, 5, 6])) &
                    (~colab_data['TIPO_TURNO_PREV'].str.contains('^L', case=False, na=False)) &
                    (~colab_data['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False))
                )
                lres_at = lres_condition.sum()
                
                # CXX_at calculation
                cxx_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (colab_data['HORARIO'].str.contains('^L', case=False, na=False)) &
                    (colab_data['WD'].isin([2, 3, 4, 5, 6])) &
                    (colab_data['TIPO_TURNO_PREV'].str.contains('^L', case=False, na=False))
                )
                cxx_at = cxx_condition.sum()
                
                count_ldt_45d_data.append({
                    'COLABORADOR': colab,
                    'LD_at': ld_at,
                    'LQ_at': lq_at, 
                    'LRES_at': lres_at,
                    'CXX_at': cxx_at
                })
            
            # Process 6 day contracts
            colabs_6d = matrizA_og[matrizA_og['tipo_contrato'] == 6]['matricula'].tolist()
            
            count_ldt_6d_data = []
            for colab in colabs_6d:
                colab_data = df_cd[df_cd['COLABORADOR'] == colab].copy()
                
                # LD_at calculation
                ld_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (colab_data['HORARIO'] != 'LQ') &
                    (colab_data['HORARIO'].str.contains('^L', case=False, na=False)) &
                    (colab_data['WD'].isin([2, 3, 4, 5, 6, 7])) &
                    (~colab_data['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False)) &
                    (colab_data['TIPO_TURNO_PREV'] != 'L')
                )
                ld_at = ld_condition.sum()
                
                # LQ_at calculation  
                lq_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (colab_data['HORARIO'] != 'LD') &
                    (colab_data['HORARIO'].str.contains('^L', case=False, na=False)) &
                    (colab_data['WD'].isin([2, 3, 4, 5, 6, 7])) &
                    (~colab_data['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False)) &
                    (colab_data['TIPO_TURNO_PREV'] != 'L')
                )
                lq_at = lq_condition.sum()
                
                # CXX_at calculation
                cxx_condition = (
                    (colab_data['DIA_TIPO'] != 'domYf') &
                    (colab_data['HORARIO'].str.contains('^L', case=False, na=False)) &
                    (colab_data['WD'].isin([2, 3, 4, 5, 6])) &
                    (colab_data['TIPO_TURNO_PREV'].str.contains('^L', case=False, na=False))
                )
                cxx_at = cxx_condition.sum()
                
                count_ldt_6d_data.append({
                    'COLABORADOR': colab,
                    'LD_at': ld_at,
                    'LQ_at': lq_at,
                    'LRES_at': 0,
                    'CXX_at': cxx_at
                })
            
            # Combine all collaborators
            count_ldt = pd.DataFrame(count_ldt_45d_data + count_ldt_6d_data)
            
            # Calculate consecutive day patterns (C2D and C3D)
            
            # Add pattern identification columns
            df_cd['FRI_SAT_SUN'] = (
                (df_cd['WDAY_PREV'] == 6) & 
                (df_cd['TIPO_TURNO_PREV'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY'] == 7) & 
                (df_cd['HORARIO'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT'] == 1) & 
                (df_cd['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False))
            )
            
            df_cd['SAT_SUN_MON'] = (
                (df_cd['WDAY'] == 7) & 
                (df_cd['HORARIO'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT'] == 1) & 
                (df_cd['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT2'] == 2) & 
                (df_cd['TIPO_TURNO_NEXT2'].str.contains('^L', case=False, na=False))
            )
            
            df_cd['SAT_SUN_ONLY'] = (
                (df_cd['WDAY'] == 7) & 
                (df_cd['HORARIO'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT'] == 1) & 
                (df_cd['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT2'] == 2) & 
                (~df_cd['TIPO_TURNO_NEXT2'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_PREV'] == 6) & 
                (~df_cd['TIPO_TURNO_PREV'].str.contains('^L', case=False, na=False))
            )
            
            df_cd['SUN_MON_ONLY'] = (
                (df_cd['WDAY'] == 7) & 
                (~df_cd['HORARIO'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT'] == 1) & 
                (df_cd['TIPO_TURNO_NEXT'].str.contains('^L', case=False, na=False)) &
                (df_cd['WDAY_NEXT2'] == 2) & 
                (df_cd['TIPO_TURNO_NEXT2'].str.contains('^L', case=False, na=False))
            )
            
            # Count C3D patterns
            c3d_fri_sat_sun = df_cd[df_cd['FRI_SAT_SUN']].groupby('COLABORADOR')['DATA'].nunique().reset_index()
            c3d_fri_sat_sun.columns = ['COLABORADOR', 'C3D_at_FSS']
            
            c3d_sat_sun_mon = df_cd[df_cd['SAT_SUN_MON']].groupby('COLABORADOR')['DATA'].nunique().reset_index()
            c3d_sat_sun_mon.columns = ['COLABORADOR', 'C3D_at_SSM']
            
            # Merge C3D counts
            c3d_at = pd.merge(c3d_sat_sun_mon, c3d_fri_sat_sun, on='COLABORADOR', how='outer').fillna(0)
            c3d_at['C3D_at'] = c3d_at['C3D_at_SSM'] + c3d_at['C3D_at_FSS']
            c3d_at = c3d_at[['COLABORADOR', 'C3D_at']]
            
            # Count C2D patterns  
            c2d_sat_sun_only = df_cd[df_cd['SAT_SUN_ONLY']].groupby('COLABORADOR')['DATA'].nunique().reset_index()
            c2d_sat_sun_only.columns = ['COLABORADOR', 'C2D_at_SSO']
            
            c2d_sun_mon_only = df_cd[df_cd['SUN_MON_ONLY']].groupby('COLABORADOR')['DATA'].nunique().reset_index()
            c2d_sun_mon_only.columns = ['COLABORADOR', 'C2D_at_SMO']
            
            # Merge C2D counts
            c2d_at = pd.merge(c2d_sat_sun_only, c2d_sun_mon_only, on='COLABORADOR', how='outer').fillna(0)
            c2d_at['C2D_at'] = c2d_at['C2D_at_SSO'] + c2d_at['C2D_at_SMO']
            c2d_at = c2d_at[['COLABORADOR', 'C2D_at']]
            
            # Merge C2D and C3D
            c_at = pd.merge(c2d_at, c3d_at, on='COLABORADOR', how='outer').fillna(0)
            
            # Merge with count_ldt
            count_ldt = pd.merge(count_ldt, c_at, on='COLABORADOR', how='left').fillna(0)
            
            # Merge with matrizA_og
            matrizA_og = matrizA_og.merge(count_ldt, left_on='matricula', right_on='COLABORADOR', how='left').fillna(0)
            
            # Store original matrizA_og
            matrizA_og_ed = matrizA_og.copy()
            
            # Adjust C2D logic (C2D = C2D + C3D)
            matrizA_og['C2D_at'] = matrizA_og['C2D_at'] + matrizA_og['C3D_at']

            # Log the column names of matrizA_og
            self.logger.info(f"Columns in matrizA_og after processing: {matrizA_og.columns.tolist()}")
            
            # Calculate adjusted values
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['LD_at']
            matrizA_og['ld'] = np.maximum(matrizA_og['ld'] - matrizA_og['LD_at'], 0)
            matrizA_og['l_dom'] = matrizA_og['l_dom'] - matrizA_og['total_dom_fes'] - matrizA_og['total_fes']
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['total_dom_fes']
            
            # Handle holidays based on contract type
            holiday_adjustment = np.where(
                matrizA_og['tipo_contrato'] == 4,
                2 * matrizA_og['total_holidays'].apply(custom_round) / 7,
                matrizA_og['total_holidays'].apply(custom_round) / 7
            )
            matrizA_og['l_total'] = matrizA_og['l_total'] - holiday_adjustment
            
            # Handle C2D and C3D
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['C2D_at']
            matrizA_og['c2d'] = np.maximum(matrizA_og['c2d'] - matrizA_og['C2D_at'], 0)
            
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['C3D_at']
            matrizA_og['c3d'] = np.maximum(matrizA_og['c3d'] - matrizA_og['C3D_at'], 0)
            
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['LQ_at']
            matrizA_og['lq'] = np.maximum(matrizA_og['lq'] - matrizA_og['LQ_at'], 0)
            
            # Handle CXX
            matrizA_og['l_total'] = matrizA_og['l_total'] - matrizA_og['CXX_at']
            matrizA_og['cxx'] = np.maximum(matrizA_og['cxx'] - matrizA_og['CXX_at'], 0)
            
            # Create matrizA with selected columns
            matrizA = matrizA_og[[
                'unidade', 'secao', 'posto', 'fk_colaborador', 'matricula', 'out',
                'tipo_contrato', 'ciclo', 'l_total', 'l_dom', 'ld', 'lq', 'q', 
                'c2d', 'c3d', 'cxx', 'descansos_atrb', 'dyf_max_t', 'LRES_at', 'lq_og'
            ]].copy()
            
            # CONTRATOS 2/3DIAS Processing -----------------------------------------------
            
            # Create matriz2_3D for 2/3 day contracts
            contract_23_employees = matrizA[matrizA['tipo_contrato'].isin([2, 3])]['matricula'].tolist()
            
            if len(contract_23_employees) > 0:
                matriz2_3d = matriz2[matriz2['COLABORADOR'].isin(contract_23_employees)].copy()
                
                # Merge with contract type information
                contract_info = matrizA[matrizA['tipo_contrato'].isin([2, 3])][['matricula', 'tipo_contrato']]
                matriz2_3d = matriz2_3d.merge(contract_info, left_on='COLABORADOR', right_on='matricula', how='left')
                
                # Filter out unwanted HORARIO types and group by week/employee
                matriz2_3d = matriz2_3d[~matriz2_3d['HORARIO'].isin(['-', 'V', 'F'])].copy()
                
                # Count work days per week per employee (divide by 2 for morning/afternoon)
                week_counts = (matriz2_3d.groupby(['COLABORADOR', 'WW'])
                            .size()
                            .reset_index(name='count'))
                week_counts['count'] = week_counts['count'] / 2
                
                # Merge back with matriz2_3d
                matriz2_3d = matriz2_3d.merge(week_counts, on=['COLABORADOR', 'WW'], how='left')
                
                # Update HORARIO based on count and contract type
                def update_horario_3d(row):
                    if row['count'] == 3 and row['tipo_contrato'] == 3:
                        return 'NL3D'
                    elif row['count'] == 2 and row['tipo_contrato'] == 2:
                        return 'NL2D'
                    else:
                        return row['HORARIO']
                
                matriz2_3d['HORARIO'] = matriz2_3d.apply(update_horario_3d, axis=1)
                
                # Remove unnecessary columns
                matriz2_3d = matriz2_3d.drop(['count', 'tipo_contrato_y'], axis=1, errors='ignore')
                
                # Merge back with main matriz2
                # First remove the employees that were processed
                matriz2 = matriz2[~matriz2['COLABORADOR'].isin(contract_23_employees)].copy()
                # Then add back the processed data
                matriz2 = pd.concat([matriz2, matriz2_3d], ignore_index=True)
            
            # Convert DATA back to string
            matriz2['DATA'] = matriz2['DATA'].astype(str)
            
            # Calculate work weeks for 2/3 day contract employees
            if len(contract_23_employees) > 0:
                matriz3d = matriz2[matriz2['COLABORADOR'].isin(contract_23_employees)].copy()
                
                # Filter work days and count weeks
                matriz3d = matriz3d[~matriz3d['HORARIO'].isin(['-', 'V', 'F', 'NL3D', 'NL2D'])].copy()
                
                work_weeks = (matriz3d.groupby('COLABORADOR')['WW']
                            .nunique()
                            .reset_index(name='count'))
                work_weeks.columns = ['matricula', 'count']
                
                # Merge with matrizA
                matrizA = matrizA.merge(work_weeks, on='matricula', how='left')
                
                # Update L_TOTAL for 2/3 day contracts
                matrizA.loc[matrizA['tipo_contrato'].isin([2, 3]), 'l_total'] = (
                    matrizA.loc[matrizA['tipo_contrato'].isin([2, 3]), 'count']
                )
            else:
                matrizA['count'] = 0
            
            matrizA = matrizA.fillna(0)
            matrizA = matrizA.drop('count', axis=1, errors='ignore')
            # Apply clip to all numeric columns explicitly
            numeric_cols = matrizA.select_dtypes(include=['number']).columns
            matrizA[numeric_cols] = matrizA[numeric_cols].clip(lower=0)
            
            # Add L_RES column
            matrizA['l_res'] = 0
            
            # Calculate auxiliary columns
            matrizA['aux'] = matrizA['l_dom'] + matrizA['ld'] + matrizA['lq'] + matrizA['c2d'] + matrizA['c3d'] + matrizA['cxx']
            matrizA['aux2'] = matrizA['l_total'] - matrizA['aux']
            
            # Adjust LD if aux2 is negative
            matrizA.loc[matrizA['aux2'] < 0, 'ld'] = matrizA['ld'] + matrizA['aux2']
            matrizA = matrizA.drop(['aux', 'aux2'], axis=1)
            
            # Set L_RES for contract type 3
            matrizA.loc[matrizA['tipo_contrato'] == 3, 'l_res'] = matrizA.loc[matrizA['tipo_contrato'] == 3, 'l_total']
            
            # Rename columns to match R output
            matrizA = matrizA.rename(columns={
                'ld': 'l_d',
                'lq': 'l_q', 
                'q': 'l_qs'
            })
            
            # Add VZ (empty days) for 4-day contracts
            matrizA['vz'] = np.where(matrizA['tipo_contrato'] == 4, semanas_restantes - 4, 0)
            
            # Update HORARIO: change 'L' to 'L_'
            matriz2.loc[matriz2['HORARIO'] == 'L', 'HORARIO'] = 'L_'
            
            # Create backup matrices
            matrizA_bk_og = matrizA.copy()
            matrizA_bk_og['l_res'] = (matrizA_bk_og['l_total'] - matrizA_bk_og['l_dom'] - 
                                    matrizA_bk_og['l_d'] - matrizA_bk_og['l_q'] - 
                                    matrizA_bk_og['l_qs'] - matrizA_bk_og['c2d'] - 
                                    matrizA_bk_og['c3d'] - matrizA_bk_og['cxx'] - 
                                    matrizA_bk_og['vz'] - matrizA_bk_og['LRES_at'])
            matrizA_bk_og['l_total'] = matrizA_bk_og['l_total'] - matrizA_bk_og['LRES_at']
            matrizA_bk_og = matrizA_bk_og.drop('LRES_at', axis=1)
            matrizA_bk_og.loc[matrizA_bk_og['l_res'] < 0, 'vz'] = matrizA_bk_og['vz'] + matrizA_bk_og['l_res']
            
            matrizA_bk = matrizA.copy()
            matrizA_bk['l_res'] = (matrizA_bk['l_total'] - matrizA_bk['l_dom'] - 
                                matrizA_bk['l_d'] - matrizA_bk['l_q'] - 
                                matrizA_bk['l_qs'] - matrizA_bk['c2d'] - 
                                matrizA_bk['c3d'] - matrizA_bk['cxx'] - 
                                matrizA_bk['vz'] - matrizA_bk['LRES_at'])
            matrizA_bk['l_total'] = matrizA_bk['l_total'] - matrizA_bk['LRES_at']
            matrizA_bk = matrizA_bk.drop('LRES_at', axis=1)
            matrizA_bk.loc[matrizA_bk['l_res'] < 0, 'vz'] = matrizA_bk['vz'] + matrizA_bk['l_res']
            
            # Sort by L_TOTAL descending and clip negative values
            matrizA_bk = matrizA_bk.sort_values('l_total', ascending=False)
            # Before clipping, identify numeric columns
            numeric_cols = matrizA_bk.select_dtypes(include=['number']).columns
            # Apply clip only to numeric columns
            matrizA_bk[numeric_cols] = matrizA_bk[numeric_cols].clip(lower=0)
            
            # Create backup for matriz_data_turno (placeholder)
            matriz_data_turno_bk = pd.DataFrame({'COLUNA': [np.nan]})
            
            # Dev TIPO TURNO FIX ---------------------------------------------------------------------
            # Fix TIPO_TURNO for consistency
            matriz2_tipo_turno_fix = (matriz2[matriz2['HORARIO'].isin(['H', 'DFS'])]
                                    [['WW', 'TIPO_TURNO']]
                                    .drop_duplicates('WW')
                                    .rename(columns={'TIPO_TURNO': 'TIPO_TURNO_FIX'}))
            
            matriz2 = matriz2.merge(matriz2_tipo_turno_fix, on='WW', how='left')
            matriz2.loc[matriz2['TIPO_TURNO'] == 'NL', 'TIPO_TURNO'] = matriz2['TIPO_TURNO_FIX']
            matriz2 = matriz2.drop('TIPO_TURNO_FIX', axis=1, errors='ignore')
            
            matriz2_bk = matriz2.copy()
            
            # Individual Employee Processing Loop ----------------------------------------
            
            for colab in matrizA_bk['matricula']:
                mmA = matrizA_bk[matrizA_bk['matricula'] == colab].copy()
                if len(mmA) == 0:
                    continue
                    
                mmA = mmA.iloc[0:1].copy()  # Take first row if multiple
                tipo_contrato = mmA['tipo_contrato'].iloc[0]
                ciclo = mmA['ciclo'].iloc[0] if 'ciclo' in mmA.columns else ''
                dyf_max_t = mmA['dyf_max_t'].iloc[0] if 'dyf_max_t' in mmA.columns else 0
                
                if tipo_contrato == 2:
                    # Process 2-day contracts using calcular_folgas2
                    new_c = matriz2_bk[matriz2_bk['COLABORADOR'] == colab].copy()
                    new_c = new_c.sort_values(['COLABORADOR', 'DATA', 'TIPO_TURNO'], ascending=[True, True, False])
                    
                    # Remove duplicates by keeping first occurrence per COLABORADOR/DATA
                    new_c = new_c.groupby(['COLABORADOR', 'DATA']).first().reset_index()
                    
                    # Group by week and calculate folgas
                    week_results = []
                    for ww in new_c['WW'].unique():
                        week_data = new_c[new_c['WW'] == ww]
                        folgas_result = calcular_folgas2(week_data)
                        folgas_result['COLABORADOR'] = colab
                        week_results.append(folgas_result)
                    
                    # Combine results
                    if week_results:
                        combined_results = pd.concat(week_results, ignore_index=True)
                        total_l_res = combined_results['L_RES'].sum()
                        total_l_dom = combined_results['L_DOM'].sum()
                        total_l_total = total_l_res + total_l_dom
                        
                        mmA.loc[:, 'l_res'] = total_l_res
                        mmA.loc[:, 'l_dom'] = total_l_dom
                        mmA.loc[:, 'l_total'] = total_l_total
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
                    
                elif tipo_contrato == 3:
                    # Process 3-day contracts using calcular_folgas3
                    new_c = matriz2_bk[matriz2_bk['COLABORADOR'] == colab].copy()
                    new_c = new_c.sort_values(['COLABORADOR', 'DATA', 'TIPO_TURNO'], ascending=[True, True, False])
                    
                    # Remove duplicates
                    new_c = new_c.groupby(['COLABORADOR', 'DATA']).first().reset_index()
                    
                    # Group by week and calculate folgas
                    week_results = []
                    for ww in new_c['WW'].unique():
                        week_data = new_c[new_c['WW'] == ww]
                        folgas_result = calcular_folgas3(week_data)
                        folgas_result['COLABORADOR'] = colab
                        week_results.append(folgas_result)
                    
                    # Combine results
                    if week_results:
                        combined_results = pd.concat(week_results, ignore_index=True)
                        total_l_res = combined_results['L_RES'].sum()
                        total_l_dom = combined_results['L_DOM'].sum()
                        total_l_total = total_l_res + total_l_dom
                        
                        mmA.loc[:, 'l_res'] = total_l_res
                        mmA.loc[:, 'l_dom'] = total_l_dom
                        mmA.loc[:, 'l_total'] = total_l_total
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
                    
                elif dyf_max_t == 0 and ciclo not in ['COMPLETO']:
                    # Employee doesn't work any Sunday - force all Sundays with L
                    mmA.loc[:, 'l_total'] = mmA['l_total'].iloc[0] - mmA['l_dom'].iloc[0]
                    mmA.loc[:, 'l_dom'] = 0
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
                    
                    # Update calendar matrix with domYfes with L
                    new_c = matriz2_bk[matriz2_bk['COLABORADOR'] == colab].copy()
                    new_c.loc[(new_c['DIA_TIPO'] == 'domYf') & (new_c['HORARIO'] != 'V'), 'HORARIO'] = 'L_DOM'
                    
                    matriz2_bk = matriz2_bk[matriz2_bk['COLABORADOR'] != colab]
                    matriz2_bk = pd.concat([matriz2_bk, new_c], ignore_index=True)
                    
                elif ciclo in ['SIN DYF']:
                    # Special cycle: only assign Sundays and LD
                    mmA.loc[:, 'c2d'] = mmA['c2d'].iloc[0] - mmA['c3d'].iloc[0]
                    mmA.loc[:, 'l_total'] = (mmA['l_dom'].iloc[0] + mmA['l_d'].iloc[0] + 
                                            mmA['c2d'].iloc[0] + mmA['cxx'].iloc[0])
                    mmA.loc[:, 'l_q'] = 0
                    mmA.loc[:, 'l_qs'] = 0
                    mmA.loc[:, 'c3d'] = 0
                    mmA.loc[:, 'l_res'] = 0
                    mmA.loc[:, 'vz'] = 0
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
                
                # Handle COMPLETO cycle - reset all L values
                if ciclo == 'COMPLETO':
                    # Reset all columns from position 9 onwards to 0
                    cols_to_reset = mmA.columns[8:]  # Assuming position 9 is index 8
                    mmA.loc[:, cols_to_reset] = 0
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
                
                # Handle CXX for 4/5 day contracts
                if (tipo_contrato in [4, 5] and mmA['cxx'].iloc[0] > 0 and 
                    ciclo not in ['SIN DYF', 'COMPLETO']):
                    
                    mmA.loc[:, 'l_res2'] = mmA['l_res'].iloc[0] - mmA['cxx'].iloc[0]
                    mmA.loc[:, 'l_res'] = mmA['cxx'].iloc[0]
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
                else:
                    mmA.loc[:, 'l_res2'] = 0
                    
                    # Update matrizA_bk
                    matrizA_bk = matrizA_bk[matrizA_bk['matricula'] != colab]
                    matrizA_bk = pd.concat([matrizA_bk, mmA], ignore_index=True)
            
            # Final cleanup of matrizA_bk
            matrizA_bk = matrizA_bk.drop('dyf_max_t', axis=1, errors='ignore')
            matrizA_bk = matrizA_bk.reset_index(drop=True)
            
            # Initialize logs
            logs = None
            
            # Update matriz2_bk HORARIO: L_ -> L_DOM for domYf
            matriz2_bk.loc[(matriz2_bk['DIA_TIPO'] == 'domYf') & 
                        (matriz2_bk['HORARIO'] == 'L_'), 'HORARIO'] = 'L_DOM'
            
            #CRIAR MATRIZ_B--------------------------------------------------
            
            # Calculate +H (working hours) for morning shifts
            trab_manha_data = []
            for data in matriz2_bk['DATA'].unique():
                if data == 'TIPO_DIA':
                    continue
                    
                day_data = matriz2_bk[(matriz2_bk['DATA'] == data) & 
                                    (matriz2_bk['COLABORADOR'] != 'TIPO_DIA')].copy()
                
                # Group by COLABORADOR to analyze each employee's day
                employee_counts = []
                for colab in day_data['COLABORADOR'].unique():
                    colab_data = day_data[day_data['COLABORADOR'] == colab]
                    
                    # Check if employee has M shift with H or NL
                    has_m = ((colab_data['TIPO_TURNO'] == 'M') & 
                            (colab_data['HORARIO'].str.contains('H|NL', case=False, na=False))).any()
                    
                    # Check if employee has T shift with H or NL  
                    has_t = ((colab_data['TIPO_TURNO'] == 'T') & 
                            (colab_data['HORARIO'].str.contains('H|NL', case=False, na=False))).any()
                    
                    # Calculate weight: 1 if only M, 0.5 if both M and T, 0 if no M
                    if has_m and has_t:
                        m_count = 0.5
                    elif has_m:
                        m_count = 1.0
                    else:
                        m_count = 0.0
                        
                    employee_counts.append(m_count)
                
                trab_manha_data.append({
                    'DATA': data,
                    'TURNO': 'M',
                    '+H': sum(employee_counts)
                })
            
            trab_manha = pd.DataFrame(trab_manha_data)
            
            # Calculate +H for afternoon shifts
            trab_tarde_data = []
            for data in matriz2_bk['DATA'].unique():
                if data == 'TIPO_DIA':
                    continue
                    
                day_data = matriz2_bk[(matriz2_bk['DATA'] == data) & 
                                    (matriz2_bk['COLABORADOR'] != 'TIPO_DIA')].copy()
                
                # Group by COLABORADOR to analyze each employee's day
                employee_counts = []
                for colab in day_data['COLABORADOR'].unique():
                    colab_data = day_data[day_data['COLABORADOR'] == colab]
                    
                    # Check if employee has T shift with H or NL (this is the main shift for afternoon)
                    has_m = ((colab_data['TIPO_TURNO'] == 'T') & 
                            (colab_data['HORARIO'].str.contains('H|NL', case=False, na=False))).any()
                    
                    # Check if employee has M shift with H or NL  
                    has_t = ((colab_data['TIPO_TURNO'] == 'M') & 
                            (colab_data['HORARIO'].str.contains('H|NL', case=False, na=False))).any()
                    
                    # Calculate weight: 1 if only T, 0.5 if both M and T, 0 if no T
                    if has_m and has_t:
                        m_count = 0.5
                    elif has_m:
                        m_count = 1.0
                    else:
                        m_count = 0.0
                        
                    employee_counts.append(m_count)
                
                trab_tarde_data.append({
                    'DATA': data,
                    'TURNO': 'T', 
                    '+H': sum(employee_counts)
                })
            
            trab_tarde = pd.DataFrame(trab_tarde_data)
            
            # Merge +H with matrizB for morning
            matrizB_m = matrizB_ini[matrizB_ini['turno'] == 'M'].copy()
            matrizB_m = matrizB_m.merge(trab_manha, left_on=['data', 'turno'], 
                                    right_on=['DATA', 'TURNO'], how='left')
            matrizB_m = matrizB_m.drop(['DATA', 'TURNO'], axis=1, errors='ignore')
            
            # Merge +H with matrizB for afternoon  
            matrizB_t = matrizB_ini[matrizB_ini['turno'] == 'T'].copy()
            matrizB_t = matrizB_t.merge(trab_tarde, left_on=['data', 'turno'],
                                    right_on=['DATA', 'TURNO'], how='left')
            matrizB_t = matrizB_t.drop(['DATA', 'TURNO'], axis=1, errors='ignore')
            
            # Combine morning and afternoon
            matrizB_ini = pd.concat([matrizB_m, matrizB_t], ignore_index=True)
            
            # Get param_pess_obj from external data or set default
            param_pess_obj = self.external_call_data.get('param_pessoas_objetivo', 0.5)
            
            # Calculate objective function and diff
            matrizB_ini['max_turno'] = pd.to_numeric(matrizB_ini['max_turno'], errors='coerce')
            matrizB_ini['min_turno'] = pd.to_numeric(matrizB_ini['min_turno'], errors='coerce')
            matrizB_ini['sd_turno'] = pd.to_numeric(matrizB_ini['sd_turno'], errors='coerce')
            matrizB_ini['media_turno'] = pd.to_numeric(matrizB_ini['media_turno'], errors='coerce')
            matrizB_ini['+H'] = pd.to_numeric(matrizB_ini['+H'], errors='coerce').fillna(0)
            
            # Calculate aux (coefficient of variation)
            matrizB_ini['aux'] = np.where(
                matrizB_ini['media_turno'] != 0,
                matrizB_ini['sd_turno'] / matrizB_ini['media_turno'],
                0
            )
            
            # Calculate pessObj (target people)
            matrizB_ini['pess_obj'] = np.where(
                matrizB_ini['aux'] >= param_pess_obj,
                np.ceil(matrizB_ini['media_turno']),
                np.round(matrizB_ini['media_turno'])
            )
            
            # Calculate diff (difference between actual and target)
            matrizB_ini['diff'] = matrizB_ini['+H'] - matrizB_ini['pess_obj']
            
            # Ensure min_turno is at least 1
            matrizB_ini['min_turno'] = np.where(matrizB_ini['min_turno'] == 0, 1, matrizB_ini['min_turno'])
            
            # Create final matrizB
            matrizB = matrizB_ini.copy()
            
            # Add weekday
            matrizB['data'] = pd.to_datetime(matrizB['data'])
            matrizB['WDAY'] = matrizB['data'].dt.dayofweek + 1
            
            # Create backup
            matrizB_bk = matrizB.copy()
            
            # Calculate holiday work statistics
            matrizA_bk_with_dyf = matrizA_bk.merge(
                matrizA_bk_og[['matricula', 'dyf_max_t']], 
                on='matricula', 
                how='left'
            )
            
            # Calculate festH (holiday work hours)
            fest_h_data = []
            for colab in matrizA_bk['matricula']:
                colab_data = matriz2_bk[
                    (matriz2_bk['COLABORADOR'] == colab) & 
                    (matriz2_bk['DIA_TIPO'] == 'domYf') & 
                    (matriz2_bk['WDAY'] != 1)
                ].copy()
                
                if len(colab_data) > 0:
                    # Group by DATA and check for work indicators
                    daily_work = colab_data.groupby('DATA').agg({
                        'HORARIO': lambda x: (x.str.contains('H|NL', case=False, na=False)).any()
                    }).reset_index()
                    daily_work.columns = ['DATA', 'dias_h']
                    
                    # Add OUT and DFS indicators
                    out_dfs_data = colab_data.groupby('DATA').agg({
                        'HORARIO': lambda x: ((x == 'OUT') | (x == 'DFS')).any()
                    }).reset_index()
                    out_dfs_data.columns = ['DATA', 'nl_out_dfs']
                    
                    daily_work = daily_work.merge(out_dfs_data, on='DATA', how='left')
                    daily_work['dias_h'] = daily_work['dias_h'] | daily_work['nl_out_dfs']
                    
                    fest_h = daily_work['dias_h'].sum()
                else:
                    fest_h = 0
                
                fest_h_data.append({'matricula': colab, 'fest_h': fest_h})
            
            fest_h_df = pd.DataFrame(fest_h_data)
            
            # Merge with matrizA_bk
            matrizA_bk = matrizA_bk.merge(fest_h_df, on='matricula', how='left')
            matrizA_bk['fest_h'] = matrizA_bk['fest_h'].fillna(0)
            
            # Calculate minFestH based on DyF_MAX_T
            def calculate_min_fest_h(row):
                dyf_max_t = row['dyf_max_t'] if 'dyf_max_t' in row else 0
                fest_h = row['fest_h']
                
                if dyf_max_t == 33:
                    return round(fest_h * 0.6)
                elif dyf_max_t == 22:
                    return round(fest_h * 0.5)
                elif dyf_max_t == 5:
                    return round(fest_h * 0.3)
                else:
                    return 0
            
            matrizA_bk['min_fest_h'] = matrizA_bk.apply(calculate_min_fest_h, axis=1)
            matrizA_bk['obj_fes'] = matrizA_bk['fest_h'] - matrizA_bk['min_fest_h']
            
            # Drop intermediate columns
            matrizA_bk = matrizA_bk.drop(['fest_h', 'obj_fes'], axis=1, errors='ignore')
            
            self.logger.info("func_inicializa MatrizB creation completed successfully")
            
            # Store final results in transformed_data
            self.medium_data.update({
                'matrizA_bk': matrizA_bk.copy(),
                'matriz2_bk': matriz2_bk.copy(), 
                'matrizB_bk': matrizB_bk.copy(),
                'tipos_de_turno': tipos_de_turno,
                'matrizA': matrizA.copy(),
                'matrizA_bk_og': matrizA_bk_og.copy(),
                'matriz_data_turno_bk': matriz_data_turno_bk.copy(),
                'logs': logs
            })
            
            self.logger.info("func_inicializa completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in func_inicializa: {str(e)}", exc_info=True)
            return False
        
    def validate_func_inicializa(self) -> bool:
        """
        Validates func_inicializa operations. Validates data before running the allocation cycle.
        """
        try:
            # TODO: Implement validation logic
            self.logger.info("Entered func_inicializa validation. Needs to be implemented.")
            return True
        except Exception as e:
            self.logger.error(f"Error validating func_inicializa from data manager: {str(e)}")
            return False

    def allocation_cycle(self, algorithm_name: str, algorithm_params: Dict[str, Any]) -> bool:
        """
        Method responsible for running the defined algorithms.
        Args:
            decision: The decision to be made (e.g., 'allocate', 'reallocate').
            parameters: Dictionary of parameters for the algorithms.
        """

        try:
            self.logger.info(f"Running allocation cycle with algorithm name: {algorithm_name} and parameters: {algorithm_params}")
            
            algorithm = AlgorithmFactory.create_algorithm(
                decision=algorithm_name,
                parameters=algorithm_params
            )

            if not algorithm:
                self.logger.error(f"Algorithm {algorithm_name} not found or could not be created.")
                return False
            
            results = algorithm.run(self.medium_data)

            if not results:
                self.logger.error(f"Algorithm {algorithm_name} returned no results.")
                return False

            if results.get('status') == 'completed':
                self.logger.error(f"Algorithm {algorithm_name} failed to run.")
                return False
            
            self.rare_data['df_results'] = results.get('result_df', {}) # TODO: define in the algorithm how the results come
            # TODO: add more data to rare_data if needed
            self.logger.info(f"Allocation cycle completed successfully with algorithm {algorithm_name}.")
            return True
        
        except Exception as e:
            self.logger.error(f"Error in allocation_cycle (data model): {str(e)}", exc_info=True)
            return False

    def validate_allocation_cycle(self) -> bool:
        """
        Validates func_inicializa operations. Validates data before running the allocation cycle.
        """
        # TODO: Where should it be? here or after formatting results
        try:
            # TODO: Implement validation logic
            self.logger.info("Entered func_inicializa validation. Needs to be implemented.")
            return True
        except Exception as e:
            self.logger.error(f"Error validating allocation_cycle from data manager: {str(e)}")
            return False

    def format_results(self) -> bool:
        """
        Method responsible for formatting results before inserting.
        """
        try:
            self.formated_data = self.rare_data
            self.logger.info("Entered format_results method. Needs to be implemented.")
            return True            
        except Exception as e:
            self.logger.error(f"Error performing format_results from data manager: {str(e)}")
            return False

    def validate_format_results(self) -> bool:
        """
        Method responsible for validating formatted results before inserting.
        """
        try:
            self.logger.info("Entered func_inicializa validation. Needs to be implemented.")
            return True            
        except Exception as e:
            self.logger.error(f"Error validating format_results from data manager: {str(e)}")
            return False
        
    def insert_results(self, data_manager: BaseDataManager, stmt: str = None) -> Tuple[bool, List[str]]:
        """
        Method for inserting results in the data source.
        """
        try:
            self.logger.info("Entered func_inicializa validation. Needs to be implemented.")
            return True, []  # Assuming no errors and no warnings          
        except Exception as e:
            self.logger.error(f"Error performing insert_results from data manager: {str(e)}")
            return False, []       

    def validate_insert_results(self, data_manager: BaseDataManager) -> bool:
        """
        Method for validating insertion results.
        """
        try:
            # TODO: Implement validation logic through data source
            self.logger.info("Entered func_inicializa validation. Needs to be implemented.")
            return True
        except Exception as e:
            self.logger.error(f"Error validating insert_results from data manager: {str(e)}")
            return False                    

    def transform_data(self, transformation_params: Optional[Dict[str, Any]] = None) -> bool:
        """
        Transform raw data based on specified parameters.
        
        Args:
            transformation_params: Dictionary of transformation parameters
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Transforming data")
            
            if not self.raw_data:
                self.logger.warning("No raw data to transform")
                return False
            
            # Default transformation parameters
            params = {
                'normalize_numeric': False,
                'fill_missing': False,
                'fill_method': 'mean',
                'remove_outliers': False,
                'outlier_threshold': 3.0
            }
            
            # Update with provided parameters
            if transformation_params:
                params.update(transformation_params)
            
            self.logger.info(f"Using transformation parameters: {params}")
            
            # Initialize transformed data
            self.transformed_data = {}
            
            # Transform each entity
            for entity_name, data in self.raw_data.items():
                try:
                    self.logger.info(f"Transforming entity: {entity_name}")
                    
                    # Make a copy to avoid modifying the original
                    df = data.copy() if hasattr(data, 'copy') else data
                    
                    # Handle missing values if requested
                    if params['fill_missing'] and hasattr(df, 'isna') and hasattr(df, 'fillna'):
                        # Get numeric columns
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        
                        if len(numeric_cols) > 0:
                            if params['fill_method'] == 'mean':
                                for col in numeric_cols:
                                    df[col] = df[col].fillna(df[col].mean())
                            elif params['fill_method'] == 'median':
                                for col in numeric_cols:
                                    df[col] = df[col].fillna(df[col].median())
                            elif params['fill_method'] == 'zero':
                                for col in numeric_cols:
                                    df[col] = df[col].fillna(0)
                        
                        # Fill non-numeric columns with mode
                        non_numeric_cols = df.select_dtypes(exclude=['number']).columns
                        for col in non_numeric_cols:
                            mode_value = df[col].mode()[0] if not df[col].mode().empty else None
                            if mode_value is not None:
                                df[col] = df[col].fillna(mode_value)
                    
                    # Normalize numeric columns if requested
                    if params['normalize_numeric'] and hasattr(df, 'select_dtypes'):
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        
                        for col in numeric_cols:
                            # Skip columns with all zeros or a single value
                            if df[col].std() > 0:
                                df[col] = (df[col] - df[col].mean()) / df[col].std()
                    
                    # Remove outliers if requested
                    if params['remove_outliers'] and hasattr(df, 'select_dtypes'):
                        numeric_cols = df.select_dtypes(include=['number']).columns
                        threshold = params['outlier_threshold']
                        
                        for col in numeric_cols:
                            mean = df[col].mean()
                            std = df[col].std()
                            
                            if std > 0:  # Skip constant columns
                                lower_bound = mean - (threshold * std)
                                upper_bound = mean + (threshold * std)
                                
                                # Create a mask for non-outliers
                                mask = (df[col] >= lower_bound) & (df[col] <= upper_bound)
                                
                                # Count outliers
                                outlier_count = (~mask).sum()
                                if outlier_count > 0:
                                    self.logger.info(f"Removed {outlier_count} outliers from {entity_name}.{col}")
                                    
                                    # Apply mask to remove outliers
                                    df = df[mask]
                    
                    # Store transformed data
                    self.transformed_data[entity_name] = df
                    
                    # Log transformation results
                    if hasattr(df, 'shape'):
                        self.logger.info(f"Transformed {entity_name}: {df.shape[0]} records, {df.shape[1]} columns")
                    
                except Exception as e:
                    self.logger.error(f"Error transforming {entity_name}: {str(e)}")
                    return False
            
            # Record operation
            self.operations_log.append({
                "operation": "transform_data",
                "parameters": params,
                "timestamp": datetime.now().isoformat()
            })
            
            self.logger.info("Data transformation completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error during data transformation: {str(e)}")
            return False
    
    def get_data_for_algorithm(self, entity_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Get transformed data ready for algorithm processing.
        
        Args:
            entity_names: Optional list of entity names to include
                          If None, all transformed entities are included
            
        Returns:
            Dictionary with data keyed by entity name
        """
        # Use transformed data if available, otherwise fall back to raw data
        source_data = self.transformed_data if self.transformed_data else self.raw_data
        
        if not source_data:
            self.logger.warning("No data available for algorithm")
            return {}
        
        # Filter entities if specified
        if entity_names:
            return {
                entity: data for entity, data in source_data.items()
                if entity in entity_names
            }
        
        return source_data
    
    def merge_entities(self, entity1: str, entity2: str, join_columns: Dict[str, str], 
                      join_type: str = 'inner') -> Optional[pd.DataFrame]:
        """
        Merge two entities based on specified join columns.
        
        Args:
            entity1: Name of the first entity
            entity2: Name of the second entity
            join_columns: Dictionary mapping columns from entity1 to entity2
            join_type: Type of join ('inner', 'left', 'right', 'outer')
            
        Returns:
            Merged DataFrame or None if merge fails
        """
        try:
            self.logger.info(f"Merging entities: {entity1} and {entity2}")
            
            # Get data from transformed if available, otherwise from raw
            source_data = self.transformed_data if self.transformed_data else self.raw_data
            
            # Check if entities exist
            if entity1 not in source_data or entity2 not in source_data:
                missing = []
                if entity1 not in source_data:
                    missing.append(entity1)
                if entity2 not in source_data:
                    missing.append(entity2)
                    
                self.logger.warning(f"Missing entities for merge: {', '.join(missing)}")
                return None
            
            # Get the DataFrames
            df1 = source_data[entity1]
            df2 = source_data[entity2]
            
            # Check if join columns exist
            for col1, col2 in join_columns.items():
                if col1 not in df1.columns:
                    self.logger.warning(f"Join column '{col1}' not found in {entity1}")
                    return None
                if col2 not in df2.columns:
                    self.logger.warning(f"Join column '{col2}' not found in {entity2}")
                    return None
            
            # Prepare column mappings for merge
            # For each join column pair, rename the column in df2 to match df1
            rename_map = {}
            for col1, col2 in join_columns.items():
                if col1 != col2:
                    rename_map[col2] = col1
            
            # Rename columns in df2 if needed
            if rename_map:
                df2_renamed = df2.rename(columns=rename_map)
            else:
                df2_renamed = df2
            
            # Perform the merge
            join_cols = list(join_columns.keys())
            merged = pd.merge(df1, df2_renamed, on=join_cols, how=join_type)
            
            self.logger.info(f"Merged {entity1} and {entity2}: {len(merged)} records")
            
            # Record operation
            self.operations_log.append({
                "operation": "merge_entities",
                "parameters": {
                    "entity1": entity1,
                    "entity2": entity2,
                    "join_columns": join_columns,
                    "join_type": join_type
                },
                "result_size": len(merged),
                "timestamp": datetime.now().isoformat()
            })
            
            return merged
            
        except Exception as e:
            self.logger.error(f"Error merging entities: {str(e)}")
            return None
    
    def get_entity_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about available entities.
        
        Returns:
            Dictionary with entity information
        """
        info = {}
        
        # Get information from raw data
        for entity, data in self.raw_data.items():
            if hasattr(data, 'shape'):
                info[entity] = {
                    "source": "raw",
                    "record_count": data.shape[0],
                    "column_count": data.shape[1],
                    "columns": data.columns.tolist(),
                    "dtypes": {col: str(dtype) for col, dtype in data.dtypes.items()}
                }
            else:
                info[entity] = {
                    "source": "raw",
                    "type": str(type(data))
                }
        
        # Add information from transformed data
        for entity, data in self.transformed_data.items():
            if entity in info:
                # Entity exists in raw data, update with transformed info
                if hasattr(data, 'shape'):
                    info[entity].update({
                        "transformed": True,
                        "transformed_record_count": data.shape[0],
                        "transformed_column_count": data.shape[1],
                        "transformed_columns": data.columns.tolist()
                    })
            else:
                # Entity only exists in transformed data
                if hasattr(data, 'shape'):
                    info[entity] = {
                        "source": "transformed",
                        "record_count": data.shape[0],
                        "column_count": data.shape[1],
                        "columns": data.columns.tolist(),
                        "dtypes": {col: str(dtype) for col, dtype in data.dtypes.items()}
                    }
                else:
                    info[entity] = {
                        "source": "transformed",
                        "type": str(type(data))
                    }
        
        return info
    
    def save_transformed_data(self, data_manager, entity_prefix: str = "transformed_") -> bool:
        """
        Save all transformed data to the data manager.
        
        Args:
            data_manager: The data manager instance
            entity_prefix: Prefix to add to entity names
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Saving transformed data")
            
            if not self.transformed_data:
                self.logger.warning("No transformed data to save")
                return False
            
            # Track success for each entity
            success_count = 0
            
            for entity_name, data in self.transformed_data.items():
                try:
                    # Create a new entity name with prefix
                    save_name = f"{entity_prefix}{entity_name}"
                    
                    # Save the data
                    data_manager.save_data(save_name, data)
                    
                    self.logger.info(f"Saved transformed data as {save_name}")
                    success_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error saving transformed data for {entity_name}: {str(e)}")
            
            all_success = success_count == len(self.transformed_data)
            
            if all_success:
                self.logger.info("All transformed data saved successfully")
            else:
                self.logger.warning(f"Saved {success_count}/{len(self.transformed_data)} transformed entities")
            
            return all_success
            
        except Exception as e:
            self.logger.error(f"Error saving transformed data: {str(e)}")
            return False

