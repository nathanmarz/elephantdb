(ns elephantdb.deploy.crate.raid0
  (:use [pallet.resource.package :only (package)]
        [pallet.resource.exec-script :only (exec-checked-script)]
        [pallet.phase :only (phase-fn)]))

(def m1-large-raid0 
     (phase-fn
      (package "mdadm")
      (package "xfsprogs")
      (exec-checked-script
       "Setting up RAID0 at '/mnt' using 2 ephemeral drives"
       ("cd /")
       ("umount /dev/sdb || echo /dev/sdb not mounted")
       ("umount /dev/sdc || echo /dev/sdc not mounted")
       ("yes | mdadm --create /dev/md0 --level=0 -c256 --raid-devices=2 /dev/sdb /dev/sdc")
       ("echo 'DEVICE /dev/sdb /dev/sdc' > /etc/mdadm.conf")
       ("mdadm --detail --scan >> /etc/mdadm.conf")
       ("blockdev --setra 65536 /dev/md0")
       ("mkfs.xfs -f /dev/md0")
       ("mkdir -p /mnt")
       ("mount -t xfs -o noatime /dev/md0 /mnt"))))