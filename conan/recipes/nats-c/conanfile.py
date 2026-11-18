import os
from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import copy, get


class NatsCConan(ConanFile):
    name = "nats-c"
    version = "3.14.0"
    package_type = "static-library"
    license = "Apache-2.0"
    homepage = "https://github.com/nats-io/nats.c"
    settings = "os", "arch", "compiler", "build_type"
    options = {"fPIC": [True, False]}
    default_options = {"fPIC": True}

    def requirements(self):
        self.requires("openssl/3.6.3")

    def layout(self):
        cmake_layout(self)

    def package_id(self):
        self.info.settings.compiler.rm_safe("cppstd")
        self.info.settings.compiler.rm_safe("libcxx")

    def source(self):
        source = self.conan_data["sources"][str(self.version)]
        base = os.getenv("DEPENDENCY_GITHUB_RAW_URL", "https://github.com").rstrip("/")
        get(self, url=source["url"].replace("https://github.com", base, 1),
            sha256=source["sha256"], strip_root=True)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables.update({"NATS_BUILD_WITH_TLS": True,
                             "NATS_BUILD_TLS_FORCE_HOST_VERIFY": True,
                             "NATS_BUILD_LIB_STATIC": True,
                             "NATS_BUILD_LIB_SHARED": False,
                             "NATS_BUILD_EXAMPLES": False,
                             "NATS_BUILD_STREAMING": False,
                             "BUILD_TESTING": False,
                             "CMAKE_POSITION_INDEPENDENT_CODE": self.options.fPIC})
        tc.generate()
        CMakeDeps(self).generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, "LICENSE", src=self.source_folder,
             dst=os.path.join(self.package_folder, "licenses"))
        CMake(self).install()

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "cnats")
        self.cpp_info.set_property("cmake_target_name", "cnats::nats_static")
        self.cpp_info.libs = ["nats_staticd" if self.settings.build_type == "Debug" else "nats_static"]
        self.cpp_info.defines = ["NATS_STATIC"]
        if self.settings.os in ("Linux", "FreeBSD"):
            self.cpp_info.system_libs = ["pthread"]
