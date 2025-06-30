"""Example service implementation for the my_new_project project.

This service demonstrates how to use the process management framework to create
a coordinated multi-stage data processing flow.
"""

import logging
from typing import Dict, Any, Optional, List, Union, Type, cast
from datetime import datetime
import pandas as pd

# Import base_data_project components
from src.algorithms.factory import AlgorithmFactory
from base_data_project.data_manager.managers.base import BaseDataManager
from base_data_project.process_management.manager import ProcessManager
from base_data_project.process_management.stage_handler import ProcessStageHandler
from base_data_project.service import BaseService
from base_data_project.storage.containers import BaseDataContainer
from base_data_project.storage.models import BaseDataModel
from base_data_project.log_config import get_logger

# Import project-specific components
from src.config import PROJECT_NAME, CONFIG
from src.models import DescansosDataModel
from src.algorithms.factory import AlgorithmFactory

class AlgoritmoGDService(BaseService):
    """
    Example service class that demonstrates how to coordinate data management,
    process tracking, and algorithm execution.
    
    This service implements a complete process flow with multiple stages:
    1. Data Loading: Load data from sources
    2. Data Transformation: Clean and prepare the data
    3. Processing: Apply algorithms to the data
    4. Result Analysis: Analyze and save the results
    """

    def __init__(self, data_manager: BaseDataManager, project_name: str, process_manager: Optional[ProcessManager] = None, external_call_dict: Dict[str, Any] = {}, config: Dict[str, Any] = {}):
        """
        Initialize the service with data and process managers.
        
        Args:
            data_manager: Data manager for data operations
            process_manager: Optional process manager for tracking
        """

        # Import CONFIG if not provided
        if config is None:
            from src.config import CONFIG
            config = CONFIG
        
        # Work around the config property issue
        if process_manager:
            try:
                process_manager.config = config
            except AttributeError:
                # If config is a property without setter, use __dict__ directly
                process_manager.__dict__['config'] = config

        super().__init__(
            data_manager=data_manager, 
            process_manager=process_manager, 
            project_name=project_name,
            data_model_class=cast(BaseDataModel, DescansosDataModel)  # Tell the linter this is okay
        )

        # Storing data here to pass it to data model in the first stage (when the class is instanciated)
        self.external_data = {
            'current_process_id': external_call_dict.get('current_process_id', 0), # TODO: Check this default
            'api_proc_id': external_call_dict.get('api_proc_id', 0),                 # arg1
            'wfm_proc_id': external_call_dict.get('wfm_proc_id', 0),                 # arg2
            'wfm_user': external_call_dict.get('wfm_user', 0),                       # arg3
            'start_date': external_call_dict.get('start_date', 0),                   # arg4
            'end_date': external_call_dict.get('end_date', 0),                       # arg5
            'wfm_proc_colab': external_call_dict.get('wfm_proc_colab', 0),           # arg6
            'child_number': external_call_dict.get('child_number', 0),               # arg7
        } if external_call_dict is not None else {}

        # Process tracking
        self.stage_handler = process_manager.get_stage_handler() if process_manager else None
        self.algorithm_results = {}

        self._register_decision_points()
        
        self.logger = get_logger(project_name)
        self.logger.info(f"project_name in service init: {project_name}")
        self.logger.info("AlgoritmoGDService initialized")

    def _register_decision_points(self):
        """Register decision points from config with the process manager"""
        if not self.process_manager:
            return
            
        stages_config = CONFIG.get('stages', {})
        
        for stage_name, stage_config in stages_config.items():
            sequence = stage_config.get('sequence')
            decisions = stage_config.get('decisions', {})
            
            if decisions and sequence is not None:
                self.logger.info(f"Registering stage {stage_name} with full decisions: {decisions}")
                # Flatten all decision groups into one dict of defaults
                defaults = {}
                for decision_group in decisions.values():
                    if isinstance(decision_group, dict):
                        defaults.update(decision_group)
                
                # Register with process manager
                self.process_manager.register_decision_point(
                    stage=sequence,
                    schema=dict,  # Keep it simple with dict schema
                    required=True,
                    defaults=defaults
                )
                
                self.logger.info(f"Registered decisions for stage {stage_name} (seq: {sequence})")

    def _dispatch_stage(self, stage_name, algorithm_name = None, algorithm_params = None):
        """Dispatch to appropriate stage method."""

        # Execute the appropriate stage
        if stage_name == "data_loading":
            return self._load_process_data()
        elif stage_name == "processing":
            return self._execute_processing_stage(algorithm_name=algorithm_name, algorithm_params=algorithm_params)
        else:
            self.logger.error(f"Unknown stage name: {stage_name}")
            return False

    def _load_process_data(self) -> bool:
        """
        Execute the data loading stage.
        
        This stage loads data from the data source(s).
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("Executing data loading raw stage")
            stage_name = 'data_loading'
            # Get decisions from process manager if available
            load_entities_dict = CONFIG.get('available_entities_processing', {})
            
            # TODO: remove this, the entities are defined in the config file on a separate dict
    
            # Track progress
            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name, 
                    0.1, 
                    "Starting data loading raw"
                )
            
            # Load each entity
            self.data = DescansosDataModel(
                data_container=BaseDataContainer(config=CONFIG, project_name=PROJECT_NAME),
                project_name=PROJECT_NAME,
                external_data=self.external_data if self.external_data else {}
            )
            
            # Progress update
            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name, 
                    0.3, 
                    "Starting load raw data entities",
                    {"entities": load_entities_dict}
                )

            valid_process_loading = self.data.load_process_data(self.data_manager, load_entities_dict)

            if not valid_process_loading:
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name,
                        0.0,
                        "Failed to load raw data.",
                        {"valid_process_loading": valid_process_loading}
                    )
                return False
            
            # Progress update
            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name, 
                    0.5, 
                    "Starting to validate raw data entities",
                    {"loaded_entities": list(self.data.auxiliary_data.keys())}
                )            
            
            valid_raw_data = self.data.validate_process_data()

            if not valid_raw_data:
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name,
                        0.0,
                        "Failed to validate raw data.",
                        {'valid_raw_data': valid_raw_data}
                    )
                return False


            if self.external_data['wfm_proc_colab'] != 'NA':
                self.external_data['colab_matricula'] = self.external_data['wfm_proc_colab']
            else:
                self.external_data['colab_matricula'] = None
            
            self.logger.info("Data loading stage completed successfully")

            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name=stage_name,
                    progress=1.0,
                    message="Raw data loading complete",
                    metadata={
                        "valid_process_loading": valid_process_loading,
                        "valid_raw_data": valid_raw_data,
                        "colab_matricula": self.external_data['colab_matricula'],
                        "data_shapes": {
                            "raw_data": self.data.auxiliary_data['valid_emp'].shape if not self.data.auxiliary_data['valid_emp'].empty else None
                        }
                    }
                )

            return True
            
        except Exception as e:
            error_msg = f"Error in data loading stage: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name=stage_name,
                    progress=0.0,
                    message=error_msg
                )
            return False

    def _execute_processing_stage(self, algorithm_name: Optional[str] = None, 
                                algorithm_params: Optional[Dict[str, Any]] = None) -> bool:
        """
        Execute the processing stage using substages. These substages could divided into various methos or the logic could be applied inside this method.
        This stage demonstrates using the substage feature and includes:
        1. connection: establish a connection to data source;
        2. load_matrixes: Load dataframes containing all the data;
        3. func_inicializa: Function that initializes data transformation for each matrix;
        4. allocation_cycle: Allocation cycle for all the required days;
        5. format_results: Format results to be inserted;
        6. insert_results: Insert results to the database.
        
        Args:
            algorithm_name: Name of the algorithm to use
            algorithm_params: Parameters for the algorithm
            
        Returns:
            True if successful, False otherwise
        """
        try:
            stage_name = 'processing'
            decisions = {}
            # TODO: check if it should exit the loop if anything fails or continue
            if self.stage_handler and self.process_manager:
                stage_sequence = self.stage_handler.stages[stage_name]['sequence']
                insert_results = self.process_manager.current_decisions.get(stage_sequence, {}).get('insertions', {}).get('insert_results', False)
                #algorithm_name = self.process_manager.current_decisions.get(stage_sequence, {}).get('algorithm', {}).get('name', algorithm_name)
                #algorithm_params = self.process_manager.current_decisions.get(stage_sequence, {}).get('algorithm', {}).get('parameters', algorithm_params)
                self.logger.info(f"Looking for defaults with stage_sequence: {stage_sequence}, type: {type(stage_sequence)}")
                stage_config = CONFIG.get('stages', {}).get('processing', {})
                decisions = stage_config.get('decisions', {})

                algorithm_name = decisions.get('algorithm', {}).get('name', algorithm_name)
                algorithm_params = decisions.get('algorithm', {}).get('parameters', algorithm_params)
                insert_results = decisions.get('insertions', {}).get('insert_results', False)
                #self.logger.info(f"Found defaults: {defaults}")
                self.logger.info(f"Retrieving these values from config algorithm_name: {algorithm_name}, algorithm_params: {algorithm_params}, insert_results: {insert_results}")

                if algorithm_name is None:
                    self.logger.error("No algorithm name provided in decisions")
                    return False

                if algorithm_params is None:
                    self.logger.error("No algorithm parameters provided in decisions")
                    return False

                # Type assertions to help type checker
                assert isinstance(algorithm_name, str)
                assert isinstance(algorithm_params, dict)

            posto_id_list = self.data.auxiliary_data.get('posto_id_list', [])
            for posto_id in posto_id_list:
                if posto_id != 158: continue # TODO: remove this, just for testing purposes
                progress = 0.0
                if self.stage_handler:
                    self.stage_handler.start_substage('processing', 'connection')
                
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.1)/len(posto_id_list),
                        message="Starting the processing stage and consequent substages"
                    )
                # SUBSTAGE 1: connection
                valid_connection = self._execute_connection_substage(stage_name)
                if not valid_connection:
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=0.0,
                            message="Error connecting to data source, returning False"
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.2)/len(posto_id_list),
                        message="Valid connection established, advancing to next substage"
                    )

                # SUBSTAGE 2: load_matrices
                if self.stage_handler:
                    self.stage_handler.start_substage('processing', 'load_matrices')
                valid_loading_matrices = self._execute_load_matrices_substage(stage_name, posto_id)
                if not valid_loading_matrices:
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=0.0,
                            message="Invalid matrices loading substage, returning False"
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.3)/len(posto_id_list),
                        message="Valid matrices loading, advancing to the next substage"
                    )

                # SUBSTAGE 3: func_inicializa
                if self.stage_handler:
                    self.stage_handler.start_substage('processing', 'func_inicializa')
                valid_func_inicializa = self._execute_func_inicializa_substage(stage_name)
                if not valid_func_inicializa:
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=0.0,
                            message="Invalid result in func_inicializa substage, returning False"
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.4)/len(posto_id_list),
                        message="Valid func_inicializa, advancing to the next substage"
                    )

                # SUBSTAGE 4: allocation_cycle
                if self.stage_handler:
                    self.stage_handler.start_substage('processing', 'allocation_cycle')
                # Type assertions to help type checker
                assert isinstance(algorithm_name, str)
                assert isinstance(algorithm_params, dict)
                valid_allocation_cycle = self._execute_allocation_cycle_substage(algorithm_params=algorithm_params, stage_name=stage_name, algorithm_name=algorithm_name)
                if not valid_allocation_cycle:
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=0.0,
                            message="Invalid result in allocation_cycle substage, returning False"
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.5)/len(posto_id_list),
                        message="Valid allocation_cycle, advancing to the next substage"
                    )

                # SUBSTAGE 5: format_results
                if self.stage_handler:
                    self.stage_handler.start_substage('processing', 'format_results')
                valid_format_results = self._execute_format_results_substage(stage_name)
                if not valid_format_results:
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=0.0,
                            message="Invalid result in format_results substage, returning False"
                        )
                    return False
                if self.stage_handler:
                    self.stage_handler.track_progress(
                        stage_name=stage_name,
                        progress=(progress+0.6)/len(posto_id_list),
                        message="Valid format_results, advancing to the next substage"
                    )

                # SUBSTAGE 6: insert_results
                if insert_results:
                    if self.stage_handler:
                        self.stage_handler.start_substage('processing', 'insert_results')
                    valid_insert_results = self._execute_insert_results_substage(stage_name)
                    if not valid_insert_results:
                        if self.stage_handler:
                            self.stage_handler.track_progress(
                                stage_name=stage_name,
                                progress=0.0,
                                message="Invalid result in insert_results substage, returning False"
                            )
                        return False
                    if self.stage_handler:
                        self.stage_handler.track_progress(
                            stage_name=stage_name,
                            progress=(progress+0.7)/len(posto_id_list),
                            message="Valid insert_results, advancing to the next substage"
                        )
                        progress += 1

            # TODO: Needs to ensure it inserted it correctly?
            if self.stage_handler:
                self.stage_handler.track_progress(
                    stage_name=stage_name,
                    progress=1.0,
                    message="Finnished processing stage with success. Returnig True."
                )
            return True

        except Exception as e:
            self.logger.error(f"Error in processing stage: {str(e)}", exc_info=True)
            # TODO: add progress tracking
            return False

    def _execute_result_analysis_stage(self) -> bool:
        """
        Execute the result analysis stage.
        
        This stage analyzes the processing results and saves the output.
        
        Returns:
            True if successful, False otherwise
        """
        # Implement the logic if needed
        return True

    def _execute_connection_substage(self, stage_name: str = 'processing') -> bool:
        """
        Execute the processing substage of connection. This could be implemented as a method or directly on the _execute_processing_stage() method
        """
        try:
            substage_name = 'connection'
            self.logger.info("Connecting to data source")
            
            # Establish connection to data source
            self.data_manager.connect()
            
            # Track progress for the connection substage
            if self.stage_handler:
                self.stage_handler.track_substage_progress(
                    stage_name=stage_name, 
                    substage_name=substage_name,
                    progress=1.0,  # 100% complete
                    message="Connection established successfully"
                )
                self.stage_handler.complete_substage(
                    stage_name=stage_name, 
                    substage_name=substage_name,
                    success=True, 
                    result_data={"connection_info": "Connected to data source"}
                )
            return True
    
        except Exception as e:
            self.logger.error(f"Error connecting to data source: {str(e)}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    "data_loading", 
                    "connection", 
                    False, 
                    {"error": str(e)}
                )
            return False
        
    def _execute_load_matrices_substage(self, stage_name: str, posto_id: int) -> bool:
        """
        Execute the processing substage of load_matrices. This could be implemented as a method or directly on the _execute_processing_stage() method
        """
        self.logger.info(f"Entering loading matrices substage for posto_id: {posto_id}")

        try:
            substage_name = "load_matrices"
            if not posto_id:
                # TODO: do something, likely raise error
                self.logger.error("No posto_id provided, cannot load matrices")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name,
                        success=False,
                        result_data={
                            'posto_id': posto_id,
                            'message': "No posto_id provided"
                        }
                    )
                return False

            try:
                self.logger.info(f"Loading colaborador info for posto_id: {posto_id}")
                # Has to be in this order
                # Get colaborador info using data model (it uses the data manager)
                valid_load_colaborador_info = self.data.load_colaborador_info(
                    data_manager=self.data_manager, 
                    posto_id=posto_id
                )
                if not valid_load_colaborador_info:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False, # TODO: create a progress logic values
                            result_data={"valid_load_colaborador_info": valid_load_colaborador_info}
                        )
                    return False
                    
            except Exception as e:
                self.logger.error(f"Error loading colaborador info: {str(e)}", exc_info=True)
                self.logger.error(f"posto_id type: {type(posto_id)}")
                self.logger.error(f"data_manager type: {type(self.data_manager)}")
                if hasattr(self.data, 'auxiliary_data') and 'valid_emp' in self.data.auxiliary_data:
                    self.logger.error(f"valid_emp shape: {self.data.auxiliary_data['valid_emp'].shape}")
                    self.logger.error(f"valid_emp columns: {self.data.auxiliary_data['valid_emp'].columns.tolist()}")
                    self.logger.error(f"valid_emp dtypes: {self.data.auxiliary_data['valid_emp'].dtypes}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                return False
            
            try:
                self.logger.info(f"Loading estimativas info for posto_id: {posto_id}")
                # Get estimativas info
                valid_load_estimativas_info = self.data.load_estimativas_info(
                    data_manager=self.data_manager, 
                    posto_id=posto_id,
                    start_date=self.external_data['start_date'],
                    end_date=self.external_data['end_date']
                )
                if not valid_load_estimativas_info:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False, # TODO: create a progress logic values
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info
                            }
                        )
                    return False
            except Exception as e:
                self.logger.error(f"Error loading estimativas info: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                return False
            
            try:
                self.logger.info(f"Loading calendario info for posto_id: {posto_id}")
                # Get calendario info
                valid_load_calendario_info = self.data.load_calendario_info(
                    data_manager=self.data_manager, 
                    process_id=self.external_data['current_process_id'],
                    posto_id=posto_id,
                    start_date=self.external_data['start_date'],
                    end_date=self.external_data['end_date']
                )
                if not valid_load_calendario_info:
                    #self.logger.error("Error loading calendario info")
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info
                            }
                        )
                    return False
            
            except Exception as e:
                self.logger.error(f"Error loading calendario info: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                return False
            
            try:
                self.logger.info(f"Loading estimativas transformations for posto_id: {posto_id}")
                # Do all the merges and data transformations
                valid_estimativas_transformations = self.data.load_estimativas_transformations()
                if not valid_estimativas_transformations:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info,
                                "valid_estimativas_transformations": valid_estimativas_transformations                            
                            }
                        )
                    return False
                
            except Exception as e:
                self.logger.error(f"Error loading estimativas transformations: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                return False

            try:
                self.logger.info(f"Loading colaborador transformations for posto_id: {posto_id}")
                valid_colaborador_transformations = self.data.load_colaborador_transformations()
                if not valid_colaborador_transformations:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info,
                                "valid_estimativas_transformations": valid_estimativas_transformations,
                                "valid_colaborador_transformations": valid_colaborador_transformations                
                            }
                        )
                    return False
            
            except Exception as e:
                self.logger.error(f"Error loading colaborador transformations: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                return False
            
            try:
                self.logger.info(f"Loading calendario transformations for posto_id: {posto_id}")
                valid_calendario_transformations = self.data.load_calendario_transformations()
                if not valid_calendario_transformations:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info,
                                "valid_estimativas_transformations": valid_estimativas_transformations,
                                "valid_colaborador_transformations": valid_colaborador_transformations,
                                "valid_calendario_transformations": valid_calendario_transformations                   
                            }
                        )
                    return False
            except Exception as e:
                self.logger.error(f"Error loading calendario transformations: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                return False
            
            try:
                self.logger.info(f"Validating loaded matrices for posto_id: {posto_id}")
                # Ensure the loaded data is valid
                valid_substage = self.data.validate_matrices_loading()
                if not valid_substage:
                    if self.stage_handler:
                        self.stage_handler.complete_substage(
                            stage_name=stage_name,
                            substage_name=substage_name,
                            success=False,
                            result_data={
                                "valid_load_colaborador_info": valid_load_colaborador_info,
                                "valid_load_estimativas_info": valid_load_estimativas_info,
                                "valid_load_calendario_info": valid_load_calendario_info
                            }
                        )
                    return False
            except Exception as e:
                self.logger.error(f"Error validating loaded matrices: {str(e)}")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                return False
            
                
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name=stage_name,
                    substage_name=substage_name,
                    success=True,
                    result_data={
                            "valid_load_colaborador_info": valid_load_colaborador_info,
                            "valid_load_estimativas_info": valid_load_estimativas_info,
                            "valid_load_calendario_info": valid_load_calendario_info
                    }
                )
            
            return True

        except Exception as e:
                self.logger.error(f"Error loading matrices: {str(e)}", exc_info=True)
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name, 
                        substage_name=substage_name, 
                        success=False, 
                        result_data={"error": str(e)}
                    )
                return False

    def _execute_func_inicializa_substage(self, stage_name: str = 'processing') -> bool:
        """
        Execute the processing substage of func_inicializa. This could be implemented as a method or directly on the _execute_processing_stage() method.
        """
        try:
            substage_name = 'func_inicializa'
            self.logger.info("Initializing func inicializa substage")
            # TODO: define semanas restantes
            start_date = self.external_data.get('start_date')
            end_date = self.external_data.get('end_date')
            
            if not isinstance(start_date, str) or not isinstance(end_date, str):
                self.logger.error("Invalid start_date or end_date")
                return False
                
            success = self.data.func_inicializa(
                start_date=start_date,
                end_date=end_date,
                fer=self.data.auxiliary_data.get('df_festivos'),
            )
            if not success:
                self.logger.warning("Performing func_inicializa unsuccessful, returning False")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name,
                        substage_name=substage_name,
                        success=False
                    )
                return False
            
            if self.stage_handler:
                self.stage_handler.track_substage_progress(
                    stage_name=stage_name,
                    substage_name=substage_name,
                    progress=0.5,
                    message="func_inicializa successful, running validations"
                )
            
            validation_result = self.data.validate_func_inicializa()
            self.logger.info(f"func_inicializa returning: {validation_result}")
            if not validation_result:
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name,
                        substage_name=substage_name,
                        success=False,
                        result_data={
                            'success_func_inicializa': success,
                            'validation_result': validation_result
                        }
                    )
                return False
            
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name='processing',
                    substage_name='func_inicializa',
                    success=True,
                    result_data={
                        'func_inicializa_success': success,
                        'validation_result': validation_result,
                        'data': self.data.medium_data
                    }
                )
            return True
        
        except Exception as e:
            self.logger.error(f"Error running func_inicializa substage: {e}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    "processing", 
                    "func_inicializa", 
                    False, 
                    {"error": str(e)}
                )
            return False

    def _execute_allocation_cycle_substage(self, algorithm_params: Dict[str, Any], stage_name: str = 'processing', algorithm_name: str = 'example_algorithm') -> bool:
        """
        Execute the processing substage of allocation_cycle. This could be implemented as a method or directly on the _execute_processing_stage() method.
        """
        try:
            substage_name = 'allocation_cycle'
            msg = "Starting allocation cycle algorithm substage."
            self.logger.info(msg=msg)
            if self.stage_handler:
                self.stage_handler.track_substage_progress(
                    stage_name=stage_name,
                    substage_name=substage_name,
                    progress=0.1,
                    message=msg,
                )

            valid_algorithm_run = self.data.allocation_cycle(
                algorithm_name=algorithm_name, 
                algorithm_params=algorithm_params,
            )
            if not valid_algorithm_run:
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name,
                        substage_name=substage_name,
                        success=False,
                        result_data={
                            'valid_algorithm_run': valid_algorithm_run
                        }
                    )
                return False
            
            valid_algorithm_results = self.data.validate_allocation_cycle()
            if not valid_algorithm_results:
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name,
                        substage_name=substage_name,
                        success=False,
                        result_data={
                            'valid_algorithm_run': valid_algorithm_run,
                            'valid_algorithm_results': valid_algorithm_results
                        }
                    )
                return False

                    
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name=stage_name,
                    substage_name=substage_name,
                    success=True,
                    result_data={
                        'valid_algorithm_run': valid_algorithm_run,
                        'valid_algorithm_results': valid_algorithm_results                        
                    }
                )

            return True

        except Exception as e:
            self.logger.error(f"Error in algorithm stage: {str(e)}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    "data_loading", 
                    "allocation_cycle", 
                    False, 
                    {"error": str(e)}
                )
            return False

    def _execute_format_results_substage(self, stage_name: str) -> bool:
        """
        Execute the processing substage of format_results for insertion. This could be implemented as a method or directly on the _execute_processing_stage() method.
        """
        try:
            self.logger.info(f"Starting format_results substage for stage: {stage_name}")
            stage_name = 'format_results'
            success = self.data.format_results()
            if not success:
                self.logger.warning("Performing allocation_cycle unsuccessful, returning False")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name=stage_name,
                        substage_name='format_results',
                        success=False
                    )
                return False
            
            if self.stage_handler:
                self.stage_handler.track_substage_progress(
                    stage_name='processing',
                    substage_name='format_results',
                    progress=0.5,
                    message="format_results successful, running validations"
                )
            
            validation_result = self.data.validate_format_results()
            self.logger.info(f"format_results returning: {validation_result}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name='processing',
                    substage_name='format_results',
                    success=True,
                    result_data={
                        'format_results_success': success,
                        'validation_result': validation_result,
                        'data': self.data.formated_data
                    }
                )
            return validation_result
        except Exception as e:
            self.logger.error(f"Error in format_results substage: {str(e)}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    "processing", 
                    "format_results", 
                    False, 
                    {"error": str(e)}
                )
            return False

    def _execute_insert_results_substage(self, stage_name: str) -> bool:
        """
        Execute the processing substage of insert_result.  This could be implemented as a method or directly on the _execute_processing_stage() method.
        """
        try:
            self.logger.info(f"Starting insert_results substage for stage: {stage_name}")
            success = self.data.insert_results(data_manager=self.data_manager)
            if not success:
                self.logger.warning("Performing allocation_cycle unsuccessful, returning False")
                if self.stage_handler:
                    self.stage_handler.complete_substage(
                        stage_name='processing',
                        substage_name='insert_results',
                        success=False
                    )
                return False
            
            if self.stage_handler:
                self.stage_handler.track_substage_progress(
                    stage_name='processing',
                    substage_name='insert_results',
                    progress=0.5,
                    message="insert_results successful, running validations"
                )

            validation_result = self.data.validate_insert_results(data_manager=self.data_manager)
            self.logger.info(f"allocation_cycle returning: {validation_result}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    stage_name='processing',
                    substage_name='insert_results',
                    success=True,
                    result_data={
                        'insert_results_success': success,
                        'validation_result': validation_result,
                    }
                )
            return validation_result            
        except Exception as e:
            self.logger.error(f"Error in insert_results substage: {str(e)}")
            if self.stage_handler:
                self.stage_handler.complete_substage(
                    "processing", 
                    "insert_results", 
                    False, 
                    {"error": str(e)}
                )
            return False            

    def finalize_process(self) -> None:
        """Finalize the process and clean up any resources."""
        self.logger.info("Finalizing process")
        
        # Nothing to do if no process manager
        if not self.stage_handler:
            return
        
        # Log completion
        self.logger.info(f"Process {self.current_process_id} completed")

    def get_process_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the current process.
        
        Returns:
            Dictionary with process summary information
        """
        if self.stage_handler:
            return self.stage_handler.get_process_summary()
        else:
            return {
                "status": "no_tracking",
                "process_id": self.current_process_id
            }

    def get_stage_decision(self, stage: int, decision_name: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific decision for a stage from the process manager.
        
        Args:
            stage: Stage number
            decision_name: Name of the decision
            
        Returns:
            Decision dictionary or None if not available
        """
        if self.process_manager:
            return self.process_manager.current_decisions.get(stage, {}).get(decision_name)
        return None