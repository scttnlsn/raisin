create_table = """
CREATE TABLE `%s` (
  pk INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  id VARCHAR(36) NOT NULL,
  entity TEXT,
  UNIQUE KEY (id)
) ENGINE = InnoDB;
"""

create_index = """
CREATE TABLE `%s` (
  id VARCHAR(36) NOT NULL,
  value VARCHAR(200) NOT NULL,
  PRIMARY KEY (id, value)
) ENGINE = InnoDB;
"""
