class APIException(Exception):
  def __init__(self, message="Failed to process your request"):
    self.message = message
    super().__init__(self.message)


class MissingParameterException(Exception):
  def __init__(self, parameter_name, message="You are missing one or more required parameters"):
    self.parameter_name = parameter_name
    self.message = message
    super().__init__(self.message)

  def __str__(self):
    return f"{self.parameter_name} -> {self.message}"


class FileNotFoundException(Exception):
  def __init__(self, file_name, message="No such file or directory"):
    self.file_name = file_name
    self.message = message
    super().__init__(self.message)

  def __str__(self):
    return f"{self.message}: '{self.file_name}'"
