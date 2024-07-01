import smtplib

smtp_host = 'smtp.gmail.com'
smtp_port = 587
smtp_user = 'jaypatel.pt9@gmail.com' 
smtp_pass =  'zykcozcusdsrxuqh'
mail_from = 'airflow@example.com'
mail_to = 'jaypatel.pt9@gmail.com'

try:
    server = smtplib.SMTP(smtp_host, smtp_port)
    server.starttls()
    server.login(smtp_user, smtp_pass)
    message = f"Subject: Test Email\n\nThis is a test email."
    server.sendmail(mail_from, mail_to, message)
    #print("Connection to SMTP server successful")
    print("mail is successfully sent")

    server.quit()
except Exception as e:
    print(f"Failed to connect to SMTP server: {e}")
