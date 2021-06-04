import mailerpy as mailer


my_mailer = mailer.Mailer('smtp.gmail.com', '587', 'testrabindrasapkota@gmail.com', 'test@1234567890')
mail_receivers = ['rabindrasapkota2@gmail.com', '071bex429@ioe.edu.np']
mail_cc = ['076msdsa012.rabindra@pcampus.edu.np']
mail_subject = 'Reconciliation Job File Upload'
# mail_bcc = []


def send_mail(file_name, load_status):
    if load_status == 'SUCCESS':
        body_message = 'File {file_name} is loaded successfully'.format(file_name=file_name)
    elif load_status == 'FAILED':
        body_message = 'File {file_name} was already received. Loading failed due to duplicate file'

    my_mailer.send_mail(to_address=mail_receivers, subject=mail_subject, mail_body=body_message, mail_cc=mail_cc)

