"""File containing the class AlgorithmFactory"""

# Dependencies
import logging
from typing import Optional, Dict, Any
from base_data_project.algorithms.base import BaseAlgorithm
from base_data_project.log_config import get_logger
from base_data_project.storage.models import BaseDataModel

# Local stuff
from src.algorithms.alcampoAlgorithm import AlcampoAlgorithm
from src.algorithms.example_algorithm import ExampleAlgorithm
from src.config import PROJECT_NAME, CONFIG

logger = get_logger(PROJECT_NAME)

class AlgorithmFactory:
    """
    Factory class for creating algorithm instances
    """

    @staticmethod
    def create_algorithm(decision: str, parameters: Optional[Dict[str, Any]] = None) -> BaseAlgorithm:
        """Choose an algorithm based on user decisions"""

        if parameters is None:
            parameters = {
                'available_algorithms': CONFIG.get('available_algorithms')  # Fix: use same key name
            }
        available_algorithms_dict = CONFIG.get('available_algorithms')

        if decision.lower() not in available_algorithms_dict:
            # If decision not available, raise an exception
            msg = f"Decision made for algorithm selection not available in config file config.py. Please configure the file."
            logger.error(msg)
            raise ValueError(msg)

        if decision.lower() == 'alcampoAlgorithm':
            logger.info(f"Creating {decision.lower()} algorithm with parameters: {parameters}")
            return AlcampoAlgorithm(algo_name=decision.lower(), parameters=parameters) # TODO: define the algorithms here
        elif decision.lower() == 'FillBagsAlgorithm':
            logger.info(f"Creating {decision.lower()} algorithm with parameters: {parameters}")
            return ExampleAlgorithm(algo_name=decision.lower(), parameters=parameters)
        else:
            error_msg = f"Unsupported algorithm type: {decision}"
            logger.error(error_msg)
            raise ValueError(error_msg)