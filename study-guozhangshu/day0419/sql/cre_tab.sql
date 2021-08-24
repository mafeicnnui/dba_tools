/*
SQLyog Ultimate v11.24 (64 bit)
MySQL - 5.7.30-33-57-log : Database - test
*********************************************************************
*/


/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;

USE `test`;


/*Table structure for table `t_dmlx` */

CREATE TABLE `t_dmlx` (
  `dm` varchar(10) NOT NULL COMMENT '大类代码',
  `mc` varchar(100) NOT NULL COMMENT '大类名称',
  PRIMARY KEY (`dm`),
  KEY `idx_t_dmlx` (`dm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `t_dmlx` */

insert  into `t_dmlx`(`dm`,`mc`) values ('01','域名');
insert  into `t_dmlx`(`dm`,`mc`) values ('02','nginx配置');

/*Table structure for table `t_dmmx` */

CREATE TABLE `t_dmmx` (
  `dm` varchar(10) NOT NULL COMMENT '代码大类',
  `dmm` varchar(20) NOT NULL COMMENT '代码小类',
  `dmmc` varchar(200) NOT NULL COMMENT '小类名称',
  PRIMARY KEY (`dm`,`dmm`),
  KEY `idx_t_dmmx` (`dm`,`dmm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table `t_dmmx` */

insert  into `t_dmmx`(`dm`,`dmm`,`dmmc`) values ('01','1','prod.hopson.com.cn');
insert  into `t_dmmx`(`dm`,`dmm`,`dmmc`) values ('01','2','pre.hopson.com.cn');
insert  into `t_dmmx`(`dm`,`dmm`,`dmmc`) values ('01','3','dev.hopson.com.cn');
insert  into `t_dmmx`(`dm`,`dmm`,`dmmc`) values ('01','4','uat.hopson.com.cn');

/*Table structure for table `t_progress` */

CREATE TABLE `t_progress` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `percent` varchar(20) DEFAULT NULL,
  `log` varchar(1000) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4;


/*Table structure for table `t_server` */

CREATE TABLE `t_server` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `desc` varchar(50) DEFAULT NULL COMMENT '描述',
  `cfg` text NOT NULL COMMENT '配置',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8mb4;

/*Data for the table `t_server` */

insert  into `t_server`(`id`,`desc`,`cfg`) values (3,'数据库平台应用1','{\r\n    \"ip\":\"10.2.39.18\",\r\n    \"port\":\"65508\",\r\n    \"user\":\"hopson\"\r\n}');
insert  into `t_server`(`id`,`desc`,`cfg`) values (6,'数据库平台应用2','{\r\n    \"ip\":\"10.2.39.20\",\r\n    \"port\":\"65508\",\r\n    \"user\":\"hopson\"\r\n}');
insert  into `t_server`(`id`,`desc`,`cfg`) values (9,'数据库平台应用3','{\r\n    \"ip\":\"10.2.39.21\",\r\n    \"port\":\"65508\",\r\n    \"user\":\"hopson\"\r\n}');

/*Table structure for table `t_settings` */

CREATE TABLE `t_settings` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(100) NOT NULL,
  `value` text,
  PRIMARY KEY (`id`,`key`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8mb4;

/*Data for the table `t_settings` */

insert  into `t_settings`(`id`,`key`,`value`) values (3,'server_key','-----BEGIN RSA PRIVATE KEY-----\r\nMIIEoQIBAAKCAQEAwhEutkrc4giHHg+VihSjaQYJ+2t6an2Hg3qO8u6/RfUaC9Tm\r\nZVgwWeNE5Kf8YXqjff44Rqd+2vDmKt4pobHI/tZ+aLQ+w8VLKzZ1ZWSpTaxiKn2W\r\nMHJdx0WwTMe8I9EnnKKWuTlSo/w+57lBID3hvqchYEdsRFfMa4sWNp5fmD+zupTj\r\neWPgeyG6i6lPckPGxNhd/0GR/FjzkBkWu2FX5iReP7NLe9qY0HanuzNJSdshPjbG\r\n6voZGcQNEf6Nou1gQxbOPNCmW7Q2CEWK56KiWWkgarLQxIKDVNlyv3QPQ0XRJli6\r\nlpkdhlxBajh2adH9upJ/zA0EMVwOIkVQxs7JEwIBIwKCAQBIFQK4vLhxNl4SetEk\r\nqJR3d0WJRS14Ar1OF5QuWK1xwXATBe8s9N7CTRmWwg1IxybIZb0hjqt15HK/dxbH\r\nB4UrdD2Umrg6FhSbBZlRi8nbBYOMIASVptJf9VAchLOYRl8rjNjltjSjTxAM7QmP\r\nofTDKCJW9fUDcRFpxfJOzRwxO5XWfgqgvp+PTlC9dy1nQ26U8YGN5dQba4Z66H3E\r\n3Vfb6wOUE1kF5MQ3mAM6yQdu4DFu5pHDLauTaxsRz6njCuGmkIUcfSaCz3QWwQYl\r\nm0inA4P5P0Xo0sEkOyNFexxpd/FPmnWwwlwSQUYyZQ/AcVcwh67EX7qHIFoSzykD\r\npl2LAoGBAOLwcMNTWmGdJdLvMSrP+hlmL5M8JHpsw2fCpI2Wz4rstYZXUx1A155+\r\nlI0o6TgknnVfxq34Of9JAfqwBw+CMvmtsRBIuWzWGlZVVX3hmNkxv0Uu7Fth4+sq\r\nWzsRBf59uJBQhvVC44LRJjmtFPM9Rdroae4HtkUo+9x+7sxmtyYTAoGBANrrH41h\r\nkiANZCHtI5azp9JrMkaT2bUeYFzRUV3rKi8zMTC8LBcrJOoK+hHgVDg1zWHbPmkg\r\nWNW6BCv278husMv8Vs27FQzqV6cm6F7vjxk+oGQ6Q34XLBoEBmKQ54kzcbcm4yYB\r\nqlQN6MDmsPZN0SRjMWx4kBeckvBdAWS4ujEBAoGAdLYrXSONkUmBKqbmFgSPQEMu\r\naPpb5zCfAimWdLP1tSlHaalAr/V2NEEZMqdTXrO/NQyvUifjUBbrEzX1AKlcC2Ck\r\nNEKogR2nJRXxc/BdPH/797G7YjJX9Uj7t/ogVv7T8nKOjMNQbym74yXQQpSKU1L0\r\npk0b6Q3DW3R6z4VIP3cCgYA4Sxa99ISpKAPONaoQzxyGkJfmQ0acSaPAGJFECUVc\r\nmCKXgNgjNvrdGMP199skZZs2a5O0oer1ITRF0cihQQjj/w8Bs8OVm1heLpOGIFf/\r\nLVxxi1MvDUXazdXBkvm1kOK6EVBaOvFI0GBsHhA/VdaxldmCSuM5RYTeuNR4/E0i\r\niwKBgQDfOCZA4ZEHDm9wyg31fcNXB/+DfL1OJo/YumNli2nfv/CqRv7EGL7PXiAP\r\nIl07BQDIcEJRO8ggA/YA/u2NZysaHuzthRlyZaOXSTDq6geo40X907bXTDKnMOQK\r\n3F+2jlTXcdLIV6OYmsq5p9gOPjOUzPBfJICHTYJmVhVbiuBgVw==\r\n-----END RSA PRIVATE KEY-----');
insert  into `t_settings`(`id`,`key`,`value`) values (6,'ssh_timeout','6');
insert  into `t_settings`(`id`,`key`,`value`) values (9,'ftp_timeout','6');

/*Table structure for table `t_templete` */

CREATE TABLE `t_templete` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `description` varchar(40) DEFAULT NULL,
  `contents` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4;

/*Data for the table `t_templete` */

insert  into `t_templete`(`id`,`description`,`contents`) values (3,'nginx配置[prod]','user nginx;\r\nworker_processes auto;\r\nerror_log /var/log/nginx/error.log;\r\npid /run/nginx.pid;\r\n\r\n# Load dynamic modules. See /usr/share/nginx/README.dynamic.\r\ninclude /usr/share/nginx/modules/*.conf;\r\n\r\nevents {\r\n    worker_connections 10240;\r\n}\r\n\r\nhttp {\r\n    log_format  main  \'$remote_addr - $remote_user [$time_local] \"$request\" \'\r\n                      \'$status $body_bytes_sent \"$http_referer\" \'\r\n                      \'\"$http_user_agent\" \"$http_x_forwarded_for\"\';\r\n\r\n    access_log  /var/log/nginx/access.log  main;\r\n\r\n    sendfile            on;\r\n    tcp_nopush          on;\r\n    tcp_nodelay         on;\r\n    keepalive_timeout   65;\r\n    types_hash_max_size 2048;\r\n\r\n    include             /etc/nginx/mime.types;\r\n    default_type        application/octet-stream;\r\n\r\n    # Load modular configuration files from the /etc/nginx/conf.d directory.\r\n    # See http://nginx.org/en/docs/ngx_core_module.html#include\r\n    # for more information.\r\n    include /etc/nginx/conf.d/*.conf;\r\n    \r\n    upstream dbapi_pool  {\r\n      server $$SERVER$$:$$PORT$$ weight=1;\r\n    }\r\n\r\n    server {\r\n        listen       80 default_server;\r\n        listen       [::]:80 default_server;\r\n        server_name  _;\r\n        root         /usr/share/nginx/html;\r\n\r\n        # Load configuration files for the default server block.\r\n        include /etc/nginx/default.d/*.conf;\r\n\r\n        location / {\r\n          proxy_pass http://dbapi_pool;\r\n          proxy_redirect     off;\r\n          proxy_set_header   Host             $host;\r\n          proxy_set_header   X-Real-IP        $remote_addr;\r\n          proxy_set_header   X-Forwarded-For  $proxy_add_x_forwarded_for;\r\n          proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;\r\n          proxy_max_temp_file_size 0;\r\n          proxy_connect_timeout      90;\r\n          proxy_send_timeout         90;\r\n          proxy_read_timeout         90;\r\n          proxy_buffer_size          32k;\r\n          proxy_buffers              4 32k; \r\n          proxy_busy_buffers_size    64k;\r\n          proxy_temp_file_write_size 64k;\r\n        }\r\n\r\n        error_page 404 /404.html;\r\n            location = /40x.html {\r\n        }\r\n        error_page 500 502 503 504 /50x.html;\r\n            location = /50x.html {\r\n        }\r\n    }\r\n}');

