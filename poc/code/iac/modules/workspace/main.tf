# ==============================================================================
# Databricks Workspace Module
# ==============================================================================
# Creates VPC, subnets, and Databricks workspace
# ==============================================================================

terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.account]
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  # Use first 2 AZs for simplicity
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

# ------------------------------------------------------------------------------
# VPC
# ------------------------------------------------------------------------------
resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.project_name}-vpc"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${var.project_name}-igw"
  }
}

# ------------------------------------------------------------------------------
# Public Subnets (for NAT Gateway)
# ------------------------------------------------------------------------------
resource "aws_subnet" "public" {
  count = 2

  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, count.index)
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project_name}-public-${local.azs[count.index]}"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  count = 2

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# ------------------------------------------------------------------------------
# NAT Gateway (single for cost savings in POC)
# ------------------------------------------------------------------------------
resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "${var.project_name}-nat-eip"
  }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name = "${var.project_name}-nat"
  }

  depends_on = [aws_internet_gateway.main]
}

# ------------------------------------------------------------------------------
# Private Subnets (for Databricks clusters)
# ------------------------------------------------------------------------------
resource "aws_subnet" "private" {
  count = 2

  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, count.index + 10)
  availability_zone = local.azs[count.index]

  tags = {
    Name = "${var.project_name}-private-${local.azs[count.index]}"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }

  tags = {
    Name = "${var.project_name}-private-rt"
  }
}

resource "aws_route_table_association" "private" {
  count = 2

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# ------------------------------------------------------------------------------
# Security Group for Databricks
# ------------------------------------------------------------------------------
resource "aws_security_group" "databricks" {
  name        = "${var.project_name}-databricks-sg"
  description = "Security group for Databricks clusters"
  vpc_id      = aws_vpc.main.id

  # Allow all traffic within the security group (cluster communication)
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  # Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-databricks-sg"
  }
}

# ------------------------------------------------------------------------------
# VPC Endpoints (required for Databricks - reduces NAT costs)
# ------------------------------------------------------------------------------
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.main.id
  service_name = "com.amazonaws.${var.aws_region}.s3"

  route_table_ids = [
    aws_route_table.private.id
  ]

  tags = {
    Name = "${var.project_name}-s3-endpoint"
  }
}

resource "aws_vpc_endpoint" "sts" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.sts"
  vpc_endpoint_type   = "Interface"
  security_group_ids  = [aws_security_group.databricks.id]
  subnet_ids          = aws_subnet.private[*].id
  private_dns_enabled = true

  tags = {
    Name = "${var.project_name}-sts-endpoint"
  }
}

resource "aws_vpc_endpoint" "kinesis" {
  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${var.aws_region}.kinesis-streams"
  vpc_endpoint_type   = "Interface"
  security_group_ids  = [aws_security_group.databricks.id]
  subnet_ids          = aws_subnet.private[*].id
  private_dns_enabled = true

  tags = {
    Name = "${var.project_name}-kinesis-endpoint"
  }
}

# ------------------------------------------------------------------------------
# Databricks Network Configuration
# ------------------------------------------------------------------------------
resource "databricks_mws_networks" "this" {
  provider           = databricks.account
  account_id         = var.databricks_account_id
  network_name       = "${var.project_name}-network"
  vpc_id             = aws_vpc.main.id
  subnet_ids         = aws_subnet.private[*].id
  security_group_ids = [aws_security_group.databricks.id]
}

# ------------------------------------------------------------------------------
# Databricks Workspace
# ------------------------------------------------------------------------------
resource "databricks_mws_workspaces" "this" {
  provider                 = databricks.account
  account_id               = var.databricks_account_id
  workspace_name           = var.project_name
  aws_region               = var.aws_region
  credentials_id           = var.credentials_id
  storage_configuration_id = var.storage_configuration_id
  network_id               = databricks_mws_networks.this.network_id

  # Wait for workspace to be running
  lifecycle {
    ignore_changes = [network_id]
  }
}
