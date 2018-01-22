FROM alpine:3.7
MAINTAINER Nils Petzaell <nils.petzall@gmail.com>

ENV KRB5_CONFIG /opt/krb5/conf/krb5.conf
ENV KRB5_KDC_PROFILE /opt/krb5/conf/kdc.conf

COPY conf /opt/krb5/conf
COPY entrypoint.sh /

RUN apk --no-cache --no-progress upgrade && \
    apk --no-cache add krb5-server && \
    mkdir -p /opt/krb5/log && \
    mkdir -p /opt/krb5/data && \
    kdb5_util create -r NPETZALL.COM -s -P pa55w0rd && \
    kadmin.local addprinc -pw admin admin/admin@NPETZALL.COM && \
    kadmin.local addprinc -pw user user/user@NPETZALL.COM && \
    chmod ugo+x /entrypoint.sh

EXPOSE 88/tcp 88/udp 749

ENTRYPOINT ["/entrypoint.sh"]