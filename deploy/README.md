## Setup

TODO: Replace this with the good stuff.

1) Add keys to ~/.ssh/elephantdb and ~/.ssh/elephantdb.pub. These will be installed on EDB server.
2) Create ~/.pallet/config.clj with contents:
          (defpallet
               :services {
              :elephantdb-deploy {
                         :provider "aws-ec2"
                         :identity "XXXX"
                         :credential "XXXXX"
                         }

              :elephantdb-data {
                         :blobstore-provider "aws-s3"
                         :provider "aws-ec2"
                         :identity "XXXX"
                         :credential "XXXX"
                         }
             }
                               {:lift-fn pallet.core/parallel-lift
                   :converge-fn pallet.core/parallel-adjust-node-counts}})

3) Add configuration files for your ring to ../deploy/conf/<ring name>
   There's an example in ../deploy/conf/example

## Usage

To provision a cluster:
$ lein run --start --ring <ring name>

To deploy to existing edb cluster:
$ lein run --start --ring <ring name>


;; Local -- replace the meta with this:

{:vmfest-Debian-6.0.2.1-64bit-v0.3 {:uuid "/Users/sritchie/.vmfest/models/vmfest-vmfest-Debian-6.0.2.1-64bit-v0.3.vdi", :os-type-id "Debian_64", :sudo-password "vmfest", :no-sudo false, :username "vmfest", :os-family :debian, :os-version "6.0.2.1", :os-64-bit true, :password "vmfest", :description "Debian 6.0.2.1 (64bit) v0.3"}}
