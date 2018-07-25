-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema my_world
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema my_world
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `my_world` DEFAULT CHARACTER SET utf8 ;
USE `my_world` ;

-- -----------------------------------------------------
-- Table `my_world`.`family`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `my_world`.`family` (
  `family_id` INT NOT NULL,
  `last_name` VARCHAR(45) NOT NULL,
  `family_size` INT NULL,
  PRIMARY KEY (`family_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `my_world`.`school`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `my_world`.`school` (
  `school_id` INT NOT NULL,
  `school_name` VARCHAR(45) NOT NULL,
  `school_type` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`school_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `my_world`.`military`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `my_world`.`military` (
  `military_branch_id` INT NOT NULL,
  `branch_name` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`military_branch_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `my_world`.`company`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `my_world`.`company` (
  `company_id` INT NOT NULL,
  `company_name` VARCHAR(45) NOT NULL,
  `company_address` VARCHAR(45) NULL,
  PRIMARY KEY (`company_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `my_world`.`political_party`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `my_world`.`political_party` (
  `political_party_id` INT NOT NULL,
  `party_name` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`political_party_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `my_world`.`ethnicity`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `my_world`.`ethnicity` (
  `ethnicity_id` INT NOT NULL,
  `ethnicity_name` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`ethnicity_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `my_world`.`religion`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `my_world`.`religion` (
  `religion_id` INT NOT NULL,
  `religion_name` VARCHAR(45) NULL,
  PRIMARY KEY (`religion_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `my_world`.`prison`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `my_world`.`prison` (
  `prison_id` INT NOT NULL,
  `prison_name` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`prison_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `my_world`.`disability`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `my_world`.`disability` (
  `disability_id` INT NOT NULL,
  `disability_name` VARCHAR(45) NULL,
  PRIMARY KEY (`disability_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `my_world`.`person`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `my_world`.`person` (
  `person_id` INT NOT NULL,
  `first_name` VARCHAR(45) NOT NULL,
  `middle_name` VARCHAR(45) NULL DEFAULT NULL,
  `last_name` VARCHAR(45) NOT NULL,
  `street_address` VARCHAR(45) NULL DEFAULT NULL,
  `city` VARCHAR(45) NULL DEFAULT NULL,
  `state` VARCHAR(45) NULL DEFAULT NULL,
  `zip_code` VARCHAR(45) NULL,
  `DOB` DATETIME NOT NULL,
  `DOD` DATETIME NULL DEFAULT NULL,
  `sex` VARCHAR(45) NOT NULL,
  `annual_income` DECIMAL(2) NOT NULL DEFAULT 0.00,
  `family_id` INT NOT NULL,
  `school_id` INT NOT NULL,
  `military_branch_id` INT NOT NULL,
  `political_party_id` INT NOT NULL,
  `company_id` INT NOT NULL,
  `ethnicity_id` INT NOT NULL,
  `disability_id` INT NOT NULL,
  `prison_id` INT NOT NULL,
  `religion_id` INT NOT NULL,
  PRIMARY KEY (`person_id`),
  INDEX `fk_person_family_idx` (`family_id` ASC),
  INDEX `fk_person_school1_idx` (`school_id` ASC),
  INDEX `fk_person_military1_idx` (`military_branch_id` ASC),
  INDEX `fk_person_political_party1_idx` (`political_party_id` ASC),
  INDEX `fk_person_company1_idx` (`company_id` ASC),
  INDEX `fk_person_ethnicity1_idx` (`ethnicity_id` ASC),
  INDEX `fk_person_disability1_idx` (`disability_id` ASC),
  INDEX `fk_person_prison1_idx` (`prison_id` ASC),
  INDEX `fk_person_religion1_idx` (`religion_id` ASC),
  CONSTRAINT `fk_person_family`
    FOREIGN KEY (`family_id`)
    REFERENCES `my_world`.`family` (`family_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_person_school1`
    FOREIGN KEY (`school_id`)
    REFERENCES `my_world`.`school` (`school_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_person_military1`
    FOREIGN KEY (`military_branch_id`)
    REFERENCES `my_world`.`military` (`military_branch_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_person_political_party1`
    FOREIGN KEY (`political_party_id`)
    REFERENCES `my_world`.`political_party` (`political_party_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_person_company1`
    FOREIGN KEY (`company_id`)
    REFERENCES `my_world`.`company` (`company_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_person_ethnicity1`
    FOREIGN KEY (`ethnicity_id`)
    REFERENCES `my_world`.`ethnicity` (`ethnicity_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_person_disability1`
    FOREIGN KEY (`disability_id`)
    REFERENCES `my_world`.`disability` (`disability_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_person_prison1`
    FOREIGN KEY (`prison_id`)
    REFERENCES `my_world`.`prison` (`prison_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_person_religion1`
    FOREIGN KEY (`religion_id`)
    REFERENCES `my_world`.`religion` (`religion_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
