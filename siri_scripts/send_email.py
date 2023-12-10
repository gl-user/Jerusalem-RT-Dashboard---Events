# # import smtplib
# # from email.mime.text import MIMEText
# # from email.mime.multipart import MIMEMultipart
# #
# # # Email configuration
# # sender_email = "your_email@gmail.com"
# # receiver_email = "recipient_email@example.com"
# # subject = "Test Email"
# # body = "This is a test email sent from Python."
# #
# # # Gmail SMTP configuration
# # smtp_server = "smtp.gmail.com"
# # smtp_port = 587
# # smtp_username = "your_email@gmail.com"
# # smtp_password = "your_email_password"
# #
# # # Create the MIME object
# # message = MIMEMultipart()
# # message["From"] = sender_email
# # message["To"] = receiver_email
# # message["Subject"] = subject
# #
# # # Attach the body of the email
# # message.attach(MIMEText(body, "plain"))
# #
# # # Establish a connection with the SMTP server
# # with smtplib.SMTP(smtp_server, smtp_port) as server:
# #     # Start TLS (Transport Layer Security) mode
# #     server.starttls()
# #
# #     # Login to the email account
# #     server.login(smtp_username, smtp_password)
# #
# #     # Send the email
# #     server.sendmail(sender_email, receiver_email, message.as_string())
# #
# # print("Email sent successfully.")
# from azure.communication.email import EmailClient
# def main():
#     try:
#         connection_string = "endpoint=https://alerts-service.unitedstates.communication.azure.com/;accesskey=qykQs0IbMg90yMO6iMSTICMRIKkx9XdQb6bYrNINURGZ7L7PmhuON55tfOpCko0dihx0C5KHmuc/JhC4/LXAZw=="
#         client = EmailClient.from_connection_string(connection_string)
#
#         message = {
#             "senderAddress": "script-alerts@8ff5a4ab-94ae-4deb-bb18-3ceea8f4e6f4.azurecomm.net",
#             "recipients":  {
#                 "to": [{"address": "ayaz4049@yahoo.com",
#                         "displayName": "Customer Name"
#                         },
#                        {"address": "ayazhussain4049@gmail.com" }],
#             },
#             "content": {
#                 "subject": "Test Email",
#                 "plainText": "Hello world via email.",
#             }
#         }
#
#         poller = client.begin_send(message)
#         result = poller.result()
#
#     except Exception as ex:
#         print(ex)
# main()

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


sender_email = 'gltransitgis@gmail.com'
receiver_email = 'ayaz4049@yaho.com'
subject = 'Example Subject'
message = 'This is the body of the email.'


smtp_server = 'smtp.gmail.com'
smtp_port = 465
username = 'gltransitgis@gmail.com'
password = 'Gl2023!!'

msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = receiver_email
msg['Subject'] = subject

msg.attach(MIMEText(message, 'plain'))

context = ssl.create_default_context()

try:
    with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as smtp:
        smtp.login(username, password)

        # Send the email
        smtp.send_message(msg)
        print('Email sent successfully.')

except Exception as e:
    print(f'Error: {e}')
