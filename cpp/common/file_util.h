#pragma once

#include <filesystem>
#include <memory>
#include <string_view>

namespace carmen {

// Provides a test utility to create a unique temporary file in the file system.
// The file is automatically deleted when the TempFile instance goes out of
// scope.
class TempFile {
 public:
  // Creates a temporary file with a random name prefixed by the provided
  // prefix.
  TempFile(std::string_view prefix = "temp");
  TempFile(const TempFile&) = delete;
  TempFile(TempFile&&) = default;
  ~TempFile();

  // Obtains the path of the owned temporary file.
  const std::filesystem::path& GetPath() const;

  // Support implicit conversion to a std::filesystem::path.
  operator std::filesystem::path() const { return GetPath(); }

 private:
  std::filesystem::path path_;
};

// Provides a test utility to create a unique temporary directory in the file
// system. The directory is automatically deleted when the TempDir instance goes
// out of scope.
class TempDir {
 public:
  // Creates a temporary directory with a random name prefixed by the provided
  // prefix.
  TempDir(std::string_view prefix = "temp");
  TempDir(const TempDir&) = delete;
  TempDir(TempDir&&) = default;
  ~TempDir();

  // Obtains the path of the owned temporary file.
  const std::filesystem::path& GetPath() const;

  // Support implicit conversion to a std::filesystem::path.
  operator std::filesystem::path() const { return GetPath(); }

 private:
  std::filesystem::path path_;
};

}  // namespace carmen
