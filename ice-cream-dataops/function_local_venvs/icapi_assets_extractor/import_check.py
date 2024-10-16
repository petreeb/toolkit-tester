from cognite.client._api.functions import validate_function_folder


def main() -> None:
    validate_function_folder(
        root_path="code/",
        function_path="./handler.py",
        skip_folder_validation=False,
    )

if __name__ == "__main__":
    main()
