
How to generate certificates:

- See: https://stackoverflow.com/questions/10175812/how-to-create-a-self-signed-certificate-with-openssl

- Or simply use
    ```shell
    openssl req -x509 -newkey ed25519 -sha512 -days 3650 \
     -nodes -keyout server.key -out server.crt \
     -subj "/CN=YOURDOMAIN.com" -addext \
     "subjectAltName=DNS:*.YOURDOMAIN.com"
    ```
  and
    ```shell
    openssl req -x509 -newkey ed25519 -sha512 -days 3650 \
     -nodes -keyout client.key -out client.crt \
     -subj "/CN=YOURDOMAIN.com" -addext \
     "subjectAltName=DNS:*.YOURDOMAIN.com"
    ```

- To debug on local machines or cloud servers, replace subjectAltName with something like
    ```shell
    "subjectAltName=DNS:*.YOURDOMAIN.com,DNS:*.us-east-2.compute.internal,DNS:localhost,IP:127.0.0.1"
    ```
