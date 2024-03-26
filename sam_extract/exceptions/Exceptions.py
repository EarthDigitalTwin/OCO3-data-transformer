# Copyright 2024 California Institute of Technology (Caltech)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Base class of all exceptions
class BaseOCOException(Exception):
    def __init__(self, *args):
        Exception.__init__(self, *args)


# Base class of exceptions which should result in an input batch being discarded from input queue
class NonRetryableException(BaseOCOException):
    pass


# If all files in a batch are unreadable
class NoValidFilesException(NonRetryableException):
    pass


# If GranuleReader cannot open an input file
class ReaderException(BaseOCOException):
    pass


# Errors that should kill the whole program
class NonRecoverableError(BaseOCOException):
    pass


