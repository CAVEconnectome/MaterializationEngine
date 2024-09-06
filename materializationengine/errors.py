class MaterializationEngineException(Exception):
    """ generic error in materialization engine """

    pass


class AlignedVolumeNotFoundException(MaterializationEngineException):
    """ error raised when a aligned_volume is not found """

    pass


class DataStackNotFoundException(MaterializationEngineException):
    """ error raised when a datastack is not found """

    pass


class MaterializeAnnotationException(Exception):
    pass


class RootIDNotFoundException(MaterializeAnnotationException):
    pass


class AnnotationParseFailure(MaterializeAnnotationException):
    pass


class TaskFailure(Exception):
    pass


class WrongModelType(KeyError):
    pass


class IndexMatchError(KeyError):
    pass


class TaskNotFound(KeyError):
    """Exception raised when periodic task is not found in the
    periodic task dict.
    """

    def __init__(self, task_name: str, task_dict: dict):
        self.message = f"""
            The task {task_name} is not part of the approved tasks. Please
            pick one of the following: {task_dict}"""
        super().__init__(self.message)

    def __str__(self):
        return f"{self.message}"


class ConfigurationError(Exception):
    """Custom exception for configuration errors."""
    pass