
# Meta Graph API Client

This project provides a Python client for interacting with the Meta (formerly Facebook) Graph API. It fetches insights and posts data from Facebook and Instagram, and upserts this data into a PostgreSQL database.

## Features

- Fetches Facebook Page and Post insights
- Fetches Instagram Post and Account insights
- Supports upserting data into PostgreSQL
- Uses HashiCorp Vault for managing secrets

## Installation

1. Clone the repository:
   ```bash
   git clone <repository_url>
   ```

2. Navigate to the project directory:
   ```bash
   cd FUB-analytics
   ```

3. Create a virtual environment:
   ```bash
   python -m venv venv
   ```

4. Activate the virtual environment:
   ```bash
   # On Windows
   venv\Scripts\activate

   # On macOS/Linux
   source venv/bin/activate
   ```

5. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the script from the command line with the following parameters:

```bash
./main.py <script_type> <start_date> <end_date>
```

- `<script_type>`: `facebook` or `instagram`
- `<start_date>` and `<end_date>`: Dates in `YYYY-MM-DD` format

For example, to fetch data for Instagram from May 1, 2023, to May 10, 2023:
```bash
./main.py instagram 2023-05-01 2023-05-10
```

### Running from an IDE

If running from an IDE, the script defaults to fetching Instagram data for the last 4 days.

## Configuration

The script uses HashiCorp Vault to retrieve database and API secrets. Ensure that your Vault is properly configured and accessible.

## API Reference

- [Facebook Graph API](https://developers.facebook.com/docs/graph-api/reference/insights)
- [Instagram Graph API](https://developers.facebook.com/docs/instagram-api/reference/ig-media/)
- [Instagram User Insights API](https://developers.facebook.com/docs/instagram-api/reference/ig-user/insights)
- [Access Token Debugger](https://developers.facebook.com/tools/debug/accesstoken)

## Contributing

Feel free to submit issues or pull requests. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License.
