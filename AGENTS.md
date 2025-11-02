# Camera Archiver - Agent Guidelines and Best Practices

## Overview

This document outlines the best practices and guidelines for agents working with the Camera Archiver system. It covers development, testing, deployment, and maintenance procedures to ensure the system remains reliable, secure, and maintainable.

## Development Guidelines

### Code Structure and Organization

1. **Modular Architecture**
   - Maintain clear separation of concerns between components
   - Each class should have a single, well-defined responsibility
   - Avoid circular dependencies between modules
   - Keep functions and methods focused on a single task

2. **Type Hints and Documentation**
   - Use strict type hints for all function parameters and return values
   - Document all public methods and classes with comprehensive docstrings
   - Include examples in docstrings for complex functionality
   - Use consistent naming conventions (snake_case for variables, PascalCase for classes)

3. **Error Handling**
   - Implement comprehensive error handling at all levels
   - Use specific exception types rather than generic ones
   - Ensure resources are properly cleaned up in error cases
   - Log errors with sufficient context for debugging

4. **Thread Safety**
   - Use locks when accessing shared resources across threads
   - Avoid global mutable state when possible
   - Ensure all public methods are thread-safe
   - Test concurrent access patterns

### Component-Specific Guidelines

#### Config Class

- Validate all configuration values during initialization
- Provide clear error messages for invalid configurations
- Use immutable objects where possible
- Document all configuration options with examples

#### FileDiscovery Class

- Handle various directory structures gracefully
- Implement robust timestamp parsing with clear error messages
- Use efficient file system traversal methods
- Consider performance when scanning large directory trees

#### FileManager Class

- Implement atomic file operations where possible
- Handle file system errors gracefully
- Use appropriate permissions for created files and directories
- Ensure trash destinations don't conflict with existing files

#### Transcoder Class

- Validate all inputs before executing external processes
- Implement proper resource cleanup for subprocesses
- Handle various ffmpeg output formats
- Monitor transcoding progress accurately

#### FileProcessor Class

- Generate clear, verifiable action plans
- Execute operations in a logical sequence
- Handle partial failures gracefully
- Maintain state consistency throughout operations

## Testing Guidelines

### Test Structure

1. **Test Organization**
   - Organize tests by component, with separate test classes for each
   - Use descriptive test names that explain what is being tested
   - Group related tests using pytest markers
   - Implement fixtures for common test scenarios
   - Explicitly do NOT use `unittest` or `unittest.mock`, instead rely on `pytest` and `pytest-mock` for the `mocker`, `mocker.patch`, `MockerFixture`, and `monkeypatch` fixtures for testing, patching, and mocking
   - Leverage `pytest` parametrization to reduce boilerplate when dealing with tests that cover similar cases
   - When a test's name would shadow or be substantially similar to another existing test concisely rename it based on what it actually is testing rather than whatever function name is being tested
   - In the event two tests reside in different test category files and are substantially the same one of them should be renamed based on what category of test it is (based on its implementation) and the other should be removed to address the shadowing conflict

2. **Test Coverage**
   - Aim for high test coverage (>90%) for critical components
   - Test both happy paths and error conditions
   - Include edge cases and boundary conditions
   - Test thread safety for concurrent operations

3. **Mocking and Fixtures**
   - Mock external dependencies (file system, subprocesses)
   - Use fixtures for creating consistent test environments
   - Implement realistic test data that mirrors production scenarios
   - Ensure mocks accurately represent the behavior of real dependencies

### Test Categories

1. **Unit Tests**
   - Test individual components in isolation
   - Verify correct behavior with various inputs
   - Test error handling paths
   - Ensure proper resource cleanup

2. **Integration Tests**
   - Test component interactions
   - Verify end-to-end workflows
   - Test with realistic file structures
   - Validate data flow between components

3. **Performance Tests**
   - Test with large file sets
   - Verify memory usage stays within bounds
   - Measure processing throughput
   - Identify and address performance bottlenecks

### Test Data Management

1. **Test File Creation**
   - Use fixtures to create realistic test file structures
   - Implement helper functions for creating test files with specific timestamps
   - Ensure test data is properly cleaned up after tests
   - Use temporary directories for all file operations

2. **Mocking External Processes**
   - Create realistic mocks for ffmpeg and ffprobe
   - Simulate various process behaviors (success, failure, timeout)
   - Ensure mocks accurately represent the real process outputs
   - Test with different process execution scenarios

## Deployment Guidelines

### Environment Setup

1. **Dependencies**
   - Pin all dependency versions in requirements.txt
   - Document all system dependencies (ffmpeg, etc.)
   - Use virtual environments to isolate dependencies
   - Implement dependency checking at startup

2. **Configuration Management**
   - Use environment-specific configuration files
   - Store sensitive configuration in secure locations
   - Implement configuration validation at startup
   - Document all configuration options with examples

3. **File System Structure**
   - Ensure proper permissions on all directories
   - Implement appropriate directory structure for input/output
   - Set up log rotation to prevent disk space issues
   - Configure monitoring for disk space usage

### Monitoring and Logging

1. **Logging Configuration**
   - Implement appropriate log levels for different environments
   - Use structured logging for easier parsing
   - Include sufficient context in log messages
   - Implement log rotation to prevent disk space issues

2. **Monitoring Metrics**
   - Track processing success/failure rates
   - Monitor disk space usage
   - Measure processing times
   - Alert on error conditions

3. **Health Checks**
   - Implement health check endpoints
   - Monitor system resource usage
   - Check for stuck processes
   - Validate configuration integrity

## Maintenance Guidelines

### Regular Maintenance Tasks

1. **Log Management**
   - Regularly review and archive old logs
   - Monitor log file sizes
   - Implement automated log cleanup
   - Review error logs for recurring issues

2. **File System Maintenance**
   - Monitor disk space usage
   - Clean up temporary files
   - Verify directory permissions
   - Check for orphaned files

3. **Performance Monitoring**
   - Track processing times over time
   - Monitor memory usage
   - Identify performance regressions
   - Optimize resource usage

### Updates and Upgrades

1. **Dependency Updates**
   - Regularly update dependencies
   - Test updates in a staging environment
   - Document any breaking changes
   - Implement rollback procedures

2. **Code Updates**
   - Follow semantic versioning
   - Document all changes in release notes
   - Test thoroughly before deployment
   - Implement gradual rollout for major changes

## Security Guidelines

### File System Security

1. **Path Validation**
   - Validate all file paths to prevent directory traversal
   - Use absolute paths for all operations
   - Implement proper permission checks
   - Sanitize user-provided paths

2. **Process Execution**
   - Validate all parameters before executing external processes
   - Use whitelists for allowed parameters
   - Implement proper resource limits
   - Monitor for suspicious process behavior

### Data Protection

1. **Access Control**
   - Implement appropriate file permissions
   - Restrict access to sensitive footage
   - Use secure authentication for remote access
   - Audit access to archived footage

2. **Data Integrity**
   - Verify file integrity after operations
   - Implement checksums for critical files
   - Detect and handle corruption
   - Maintain backup copies of important data

## Troubleshooting Guidelines

### Common Issues

1. **Transcoding Failures**
   - Check ffmpeg installation and version
   - Verify input file integrity
   - Monitor system resources during transcoding
   - Review ffmpeg error logs

2. **File System Issues**
   - Check disk space availability
   - Verify directory permissions
   - Look for file system errors in logs
   - Check for file locking issues

3. **Performance Issues**
   - Monitor system resource usage
   - Check for I/O bottlenecks
   - Review processing logs for delays
   - Analyze memory usage patterns

### Debugging Procedures

1. **Log Analysis**
   - Use structured logging queries
   - Correlate events across components
   - Look for error patterns
   - Check for warning signs

2. **System Monitoring**
   - Monitor system resources
   - Check process status
   - Analyze network traffic
   - Review system logs

## Agent Responsibilities

### Development Agents

1. **Code Quality**
   - Write clean, maintainable code
   - Explicitly avoid destructive changes unless the user is prompted and confirms that prompt
   - Follow established coding standards
   - Implement comprehensive tests
   - Document all changes
   - Before marking a task list as 'completed' we should run our test suite to ensure all tests pass by using the command listed below

2. **Testing**
   - Write tests for all new functionality
   - Ensure tests pass before submitting changes
   - Maintain high test coverage
   - Fix failing tests promptly

3. Command usage
   - The test suite should be run with: `uv run pytest -v`
   - Code coverage reports can be gathered with: `uv run slipcover -m pytest -v`
   - Code linting/formatting should be run after any code changes by using: `ruff format ; ruff check --select I --fix; pyright`
   - Markdown files should be formatted after changes by using: `prettier --cache -c -w *.md`
   - When modifying our test suite we should ensure we grab an updated code coverage report by running the code coverage command listed above

### Operations Agents

1. **Monitoring**
   - Monitor system health
   - Respond to alerts promptly
   - Track performance metrics
   - Identify and address issues

2. **Maintenance**
   - Perform regular maintenance tasks
   - Keep systems updated
   - Manage log files
   - Optimize performance

### Security Agents

1. **Security Monitoring**
   - Monitor for security threats
   - Implement security best practices
   - Respond to security incidents
   - Regular security audits

2. **Access Control**
   - Manage user permissions
   - Review access logs
   - Implement secure authentication
   - Regularly update security measures

## Communication Guidelines

### Reporting Issues

1. **Bug Reports**
   - Include detailed reproduction steps
   - Provide system information
   - Attach relevant logs
   - Describe expected vs. actual behavior

2. **Feature Requests**
   - Clearly describe the desired functionality
   - Explain the use case and benefits
   - Consider implementation complexity
   - Propose potential solutions

### Documentation

1. **Code Documentation**
   - Document all public interfaces
   - Include usage examples
   - Explain design decisions
   - Keep documentation up to date

2. **Process Documentation**
   - Document operational procedures
   - Create troubleshooting guides
   - Maintain configuration guides
   - Update documentation regularly

## Conclusion

Following these guidelines will help ensure the Camera Archiver system remains reliable, secure, and maintainable. Regular review and updates to these practices will help the system evolve to meet changing requirements while maintaining high quality standards.
