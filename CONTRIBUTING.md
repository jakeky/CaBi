# Contributing to CaBi

Thank you for your interest in contributing to CaBi! This is a GitHub repository with scripts to analyze Capital Bikeshare data. The data has information on bike trips in the D.C. area. The scripts use Python and Dask to read, clean, transform, and query the data with SQL. The repository shows how to use Dask and Dask SQL for large datasets. The repository also has some query and visualization examples.

## How to contribute

There are many ways you can contribute to this project, such as:

- Reporting issues or bugs
- Suggesting new features or enhancements
- Submitting pull requests with code changes or documentation updates
- Reviewing and testing existing pull requests
- Providing feedback or suggestions

Before you start contributing, please read the following sections carefully.

## Code of conduct

We expect all contributors to follow the GitHub Community Guidelines and the Python Code of Conduct. Please be respectful, courteous, and constructive in your interactions with other contributors and users. Do not engage in any behavior that violates these guidelines or the terms of service of GitHub or any other platform. If you encounter any inappropriate behavior, please report it to the project maintainer or GitHub staff.

## License

This project is licensed under the MIT License. By contributing to this project, you agree to abide by its terms and conditions. You also agree to grant the project maintainer and other contributors a non-exclusive, royalty-free, worldwide, perpetual, and irrevocable license to use, copy, modify, distribute, and sublicense your contributions, under the same license as the project.

## Getting started

To get started with contributing to this project, you will need to:

- Fork the repository on GitHub
- Clone your forked repository to your local machine
- Install the required dependencies using `pip install -r requirements.txt`
- Run the tests using `pytest`
- Create a new branch for your feature or bug fix
- Make your changes and commit them with a descriptive message
- Push your branch to your forked repository
- Create a pull request from your branch to the original repository
- Wait for the project maintainer or other contributors to review and merge your pull request

## Coding style

We follow the PEP 8 style guide for Python code and the PEP 257 style guide for docstrings. Please make sure your code is consistent with these standards and follows the best practices for Python programming. You can use tools like flake8 or black to check and format your code. You can also use pylint or mypy to check for errors and type annotations.

## Documentation

We use Sphinx to generate the documentation for this project. The documentation source files are located in the `docs` directory. To build the documentation locally, you will need to install Sphinx and its extensions using `pip install -r docs/requirements.txt`. Then, you can run `make html` in the `docs` directory to generate the HTML files in the `docs/_build/html` directory. You can open the `index.html` file in your browser to view the documentation.

If you make any changes to the code or the documentation, please update the corresponding documentation files accordingly. You can also add new documentation files if needed, but make sure to include them in the `docs/index.rst` file. Please follow the Sphinx style guide for writing documentation.

## Testing

We use pytest as the testing framework for this project. The test files are located in the `tests` directory. To run the tests, you will need to install pytest and its plugins using `pip install -r tests/requirements.txt`. Then, you can run `pytest` in the root directory of the project to run all the tests. You can also use `pytest -v` to see more verbose output, or `pytest -k <expression>` to run only the tests that match the expression.

If you make any changes to the code, please add or update the corresponding test files accordingly. You can also add new test files if needed, but make sure to name them with the `test_` prefix and place them in the appropriate subdirectory. Please follow the pytest style guide and the pytest documentation for writing tests.

## Feedback and support

If you have any questions, comments, or suggestions, please feel free to open an issue or a discussion on GitHub. We appreciate your feedback and support. Thank you for contributing to CaBi! 
