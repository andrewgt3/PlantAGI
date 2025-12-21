# Stage 1: Build the React Application
FROM node:20-alpine AS builder

WORKDIR /app

# Install dependencies
# context is root, so we copy from frontend/
COPY frontend/package.json frontend/package-lock.json ./
RUN npm install --legacy-peer-deps

# Copy source code
COPY frontend/ ./

# Build the application
RUN npm run build

# Stage 2: Serve with Python
FROM python:3.9-slim

WORKDIR /app

# Copy built assets from builder stage
COPY --from=builder /app/dist ./frontend/dist
# Copy server script from root context
COPY server.py .

CMD ["python", "server.py"]
