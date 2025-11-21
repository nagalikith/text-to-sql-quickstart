FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed (none for now)

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose the MCP server port
EXPOSE 8080

# Default command runs the MCP server
CMD ["python", "mcp_server/run_mcp_server.py"]
