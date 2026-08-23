data "aws_availability_zones" "available" {
  count = local.create_network ? 1 : 0

  state = "available"
}

data "aws_route_table" "existing_private" {
  for_each = local.create_network || !var.create_s3_endpoint ? toset([]) : toset(var.private_subnet_ids)

  subnet_id = each.value
}

resource "aws_vpc" "this" {
  count = local.create_network ? 1 : 0

  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = var.project
  }
}

resource "aws_internet_gateway" "this" {
  count = local.create_network ? 1 : 0

  vpc_id = aws_vpc.this[0].id

  tags = {
    Name = var.project
  }
}

resource "aws_subnet" "public" {
  count = local.create_network ? 2 : 0

  vpc_id                  = aws_vpc.this[0].id
  availability_zone       = data.aws_availability_zones.available[0].names[count.index]
  cidr_block              = cidrsubnet(aws_vpc.this[0].cidr_block, 8, count.index)
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project}-public-${count.index + 1}"
  }
}

resource "aws_subnet" "private" {
  count = local.create_network ? 2 : 0

  vpc_id            = aws_vpc.this[0].id
  availability_zone = data.aws_availability_zones.available[0].names[count.index]
  cidr_block        = cidrsubnet(aws_vpc.this[0].cidr_block, 8, count.index + 10)

  tags = {
    Name = "${var.project}-private-${count.index + 1}"
  }
}

resource "aws_eip" "nat" {
  count = local.create_network ? 1 : 0

  domain = "vpc"

  tags = {
    Name = "${var.project}-nat"
  }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_nat_gateway" "this" {
  count = local.create_network ? 1 : 0

  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name = var.project
  }

  depends_on = [aws_internet_gateway.this]
}

resource "aws_route_table" "public" {
  count = local.create_network ? 1 : 0

  vpc_id = aws_vpc.this[0].id

  tags = {
    Name = "${var.project}-public"
  }
}

resource "aws_route" "public_internet" {
  count = local.create_network ? 1 : 0

  route_table_id         = aws_route_table.public[0].id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this[0].id
}

resource "aws_route_table_association" "public" {
  count = local.create_network ? 2 : 0

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_route_table" "private" {
  count = local.create_network ? 1 : 0

  vpc_id = aws_vpc.this[0].id

  tags = {
    Name = "${var.project}-private"
  }
}

resource "aws_route" "private_nat" {
  count = local.create_network ? 1 : 0

  route_table_id         = aws_route_table.private[0].id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this[0].id
}

resource "aws_route_table_association" "private" {
  count = local.create_network ? 2 : 0

  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[0].id
}

resource "aws_vpc_endpoint" "s3" {
  count = local.create_s3_endpoint ? 1 : 0

  vpc_id            = local.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids = local.create_network ? (
    [aws_route_table.private[0].id]
    ) : (
    [for rt in data.aws_route_table.existing_private : rt.id]
  )

  tags = {
    Name = "${var.project}-s3"
  }
}
