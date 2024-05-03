# This is a sample Python script demonstrating file manipulation functions

def read_file(filename):
    """
    Read content from a file.
    
    Args:
        filename (str): The name of the file to read.
        
    Returns:
        str: Content of the file.
    """
    with open(filename, "r") as file:
        content = file.read()
    return content

def write_file(filename, content):
    """
    Write content to a file.
    
    Args:
        filename (str): The name of the file to write.
        content (str): The content to write to the file.
    """
    with open(filename, "w") as file:
        file.write(content)

def main():
    filename = "sample.txt"
    content = "Hello, this is a sample text."
    
    # Writing content to a file
    write_file(filename, content)
    
    # Reading content from the file
    file_content = read_file(filename)
    print("Content read from file:", file_content)

if __name__ == "__main__":
    main()

