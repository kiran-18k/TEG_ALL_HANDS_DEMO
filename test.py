# This is a sample Python script demonstrating file manipulation functions



# API_UTILS_MATT
import logging
# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')




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
    logging.debug("Content read from file: %s", file_content)


def df_proces(df):

    return df.shape

if __name__ == "__main__":
    main()



# YUP TEST IS ON THE WAAY

