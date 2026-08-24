import os

from conan import ConanFile
from conan.tools.files import copy, get


required_conan_version = ">=2.8.0"


class UserverGoogleApisConan(ConanFile):
    name = "googleapis"
    version = "2023.5.1"
    package_type = "header-library"
    license = "Apache-2.0"
    homepage = "https://github.com/googleapis/googleapis"
    description = "Source-only Google common protos consumed by userver"

    def source(self):
        get(
            self,
            **self.conan_data["sources"][str(self.version)],
            strip_root=True,
        )

    def package(self):
        copy(
            self,
            "LICENSE",
            src=self.source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )
        for directory in ("api", "rpc", "type"):
            copy(
                self,
                "*.proto",
                src=os.path.join(self.source_folder, "google", directory),
                dst=os.path.join(
                    self.package_folder, "res", "google", directory,
                ),
            )

    def package_info(self):
        # userver's upstream recipe deliberately reads this exact component
        # and uses only its proto root. It then generates the required C++
        # sources itself in SetupGoogleProtoApis.cmake.
        component = self.cpp_info.components["google_rpc_status_proto"]
        component.resdirs = ["res"]
