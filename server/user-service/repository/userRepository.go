package repository

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
	"user-service/customLogger"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/crypto/bcrypt"
	"log"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"strings"
	"user-service/data"
	"user-service/utils"
)

type UserRepository struct {
	cli        *mongo.Client
	redis      *redis.Client
	logger     *log.Logger
	custLogger *customLogger.Logger
	tracer     trace.Tracer
}

const (
	errAccount         = "Error finding account:"
	recoveryConstruct  = "recovery:%s"
	errUser            = "Error finding user"
	successfulID       = "Successfully found ID"
	errRole            = "Error finding role"
	successfulRole     = "Successfully found role"
	successfulPassword = "Successfully changed password"
	emptyToken         = "token is empty"
	recaptchaNotSet    = "RECAPTCHA_SECRET_KEY is not set"
	recaptchaErr       = "reCAPTCHA response error"
)

func constructKeyForRecovery(a string) string {
	return fmt.Sprintf(recoveryConstruct, a)
}

func New(ctx context.Context, logger *log.Logger, custLogger *customLogger.Logger, tracer trace.Tracer) (*UserRepository, error) {
	dburi := os.Getenv("MONGO_DB_URI")

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(dburi))
	if err != nil {
		return nil, err
	}

	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisAddress := fmt.Sprintf("%s:%s", redisHost, redisPort)

	redisCli := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})

	if redisCli.Ping().Err() != nil {
		return nil, err
	}

	return &UserRepository{
		cli:        client,
		redis:      redisCli,
		logger:     logger,
		custLogger: custLogger,
		tracer:     tracer,
	}, nil
}

func (ur *UserRepository) Disconnect(ctx context.Context) error {
	_, span := ur.tracer.Start(ctx, "UserRepository.Disconnect")
	defer span.End()
	err := ur.cli.Disconnect(ctx)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())

		return err
	}
	span.SetStatus(codes.Ok, "Successfully disconnected")
	return nil
}

func hashPassword(password string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hashed), nil
}

func (ur *UserRepository) getAccountCollection() *mongo.Collection {
	userDatabase := ur.cli.Database("mongoDb")
	userCollection := userDatabase.Collection("accounts")
	return userCollection
}

func sendEmail(email string) error {
	from := os.Getenv("SMTP_EMAIL")
	password := os.Getenv("SMTP_PASSWORD")
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort := os.Getenv("SMTP_PORT")
	userService := os.Getenv("HTTPS_LINK_TO_USER")

	verificationLink := fmt.Sprintf("%s/%s", userService, email)

	plainTextBody := "Welcome to our service!\n\n" +
		"Thank you for joining our platform. Please verify your email address by clicking the link below:\n" +
		verificationLink + "\n\n" +
		"The link will expire in 10 minutes.\n\n" +
		"Best regards,\nThe Team"

	htmlBody := `<!DOCTYPE html>
	<html>
	<head>
		<style>
			body { font-family: Arial, sans-serif; color: #333; }
			.container { padding: 20px; border: 1px solid #ddd; }
			.header { font-size: 24px; font-weight: bold; color: #4CAF50; }
			.content { margin-top: 10px; }
			.button {
				display: inline-block;
				padding: 10px 20px;
				font-size: 16px;
				color: #fff;
				background-color: #4CAF50;
				text-decoration: none;
				border-radius: 5px;
				margin-top: 10px;
			}
			.footer { margin-top: 20px; font-size: 12px; color: #888; }
		</style>
	</head>
	<body>
		<div class="container">
			<div class="header">Welcome to Our Service!</div>
			<div class="content">
				<p>Thank you for joining our platform. Please verify your email address by clicking the button below:</p>
				<a href="` + verificationLink + `" class="button">Verify Email</a>
				<p>This link will expire in 10 minutes.</p>
			</div>
			<div class="footer">
				<p>Best regards,<br>The Team</p>
			</div>
		</div>
	</body>
	</html>`

	message := []byte("MIME-Version: 1.0\r\n" +
		"Content-Type: multipart/alternative; boundary=\"fancy-boundary\"\r\n" +
		"Subject: Verify Your Email Address\r\n" +
		"From: " + from + "\r\n" +
		"To: " + email + "\r\n" +
		"\r\n" +
		"--fancy-boundary\r\n" +
		"Content-Type: text/plain; charset=\"utf-8\"\r\n" +
		"\r\n" +
		plainTextBody + "\r\n" +
		"--fancy-boundary\r\n" +
		"Content-Type: text/html; charset=\"utf-8\"\r\n" +
		"\r\n" +
		htmlBody + "\r\n" +
		"--fancy-boundary--")

	auth := smtp.PlainAuth("", from, password, smtpHost)
	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, from, []string{email}, message)
	if err != nil {
		return fmt.Errorf("failed to send email: %w", err)
	}

	return nil
}

func (uh *UserRepository) GetAllManagers(ctx context.Context) (data.Accounts, error) {
	ctx, span := uh.tracer.Start(ctx, "UserRepository.GetAllManagers")
	defer span.End()

	uh.custLogger.Debug(logrus.Fields{
		"method": "GetAllManagers",
	}, "Starting GetAllManagers")

	managersCollection := uh.getAccountCollection()
	var managers data.Accounts
	filter := bson.M{"role": "manager"}

	managersCursor, err := managersCollection.Find(ctx, filter)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(err)
		uh.custLogger.Error(logrus.Fields{
			"method": "GetAllManagers",
			"error":  err.Error(),
		}, "Error finding managers")
		return nil, err
	}

	if err = managersCursor.All(ctx, &managers); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(err)
		uh.custLogger.Error(logrus.Fields{
			"method": "GetAllManagers",
			"error":  err.Error(),
		}, "Error decoding manager results")
		return nil, err
	}

	uh.custLogger.Info(logrus.Fields{
		"method": "GetAllManagers",
		"count":  len(managers),
	}, "Successfully retrieved all managers")

	span.SetStatus(codes.Ok, "Successfully found all managers")
	return managers, nil
}

func (uh *UserRepository) GetOne(ctx context.Context, userId string) (*data.Account, error) {
	ctx, span := uh.tracer.Start(ctx, "UserRepository.GetOne")
	defer span.End()

	uh.custLogger.Debug(logrus.Fields{
		"method": "GetOne",
		"userId": userId,
	}, "Starting GetOne")

	objectId, err := primitive.ObjectIDFromHex(userId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.custLogger.Error(logrus.Fields{
			"method": "GetOne",
			"userId": userId,
			"error":  err.Error(),
		}, "Invalid ObjectID format")
		return nil, err
	}

	managersCollection := uh.getAccountCollection()
	var manager data.Account
	err = managersCollection.FindOne(ctx, bson.M{"_id": objectId}).Decode(&manager)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Error finding manager:", objectId)
		uh.custLogger.Error(logrus.Fields{
			"method":   "GetOne",
			"userId":   userId,
			"objectId": objectId.Hex(),
			"error":    err.Error(),
		}, "Error finding manager by ID")
		return nil, err
	}

	uh.custLogger.Info(logrus.Fields{
		"method":   "GetOne",
		"userId":   userId,
		"objectId": objectId.Hex(),
	}, "Successfully found manager")

	span.SetStatus(codes.Ok, "Successfully found manager")
	return &manager, nil
}

func (ur *UserRepository) GetAllMembers(ctx context.Context) ([]data.Account, error) {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.GetAllMembers")
	defer span.End()

	ur.custLogger.Debug(logrus.Fields{
		"method": "GetAllMembers",
	}, "Starting GetAllMembers")

	if err := ur.cli.Ping(ctx, readpref.Primary()); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Database not available")
		ur.custLogger.Error(logrus.Fields{
			"method": "GetAllMembers",
			"error":  err.Error(),
		}, "Database ping failed")
		return nil, fmt.Errorf("database not available: %w", err)
	}

	accountCollection := ur.getAccountCollection()
	filter := bson.M{"role": "member"}

	cursor, err := accountCollection.Find(ctx, filter)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error finding accounts:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetAllMembers",
			"error":  err.Error(),
		}, "Error querying members")
		return nil, err
	}
	defer cursor.Close(ctx)

	var accounts []data.Account
	if err := cursor.All(ctx, &accounts); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error decoding accounts:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetAllMembers",
			"error":  err.Error(),
		}, "Error decoding member results")
		return nil, err
	}

	ur.custLogger.Info(logrus.Fields{
		"method": "GetAllMembers",
		"count":  len(accounts),
	}, "Successfully retrieved all members")

	span.SetStatus(codes.Ok, "Successfully found members")
	return accounts, nil
}

func (ur *UserRepository) GetUserIdByEmail(ctx context.Context, email string) (primitive.ObjectID, error) {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.GetUserIdByEmail")
	defer span.End()

	ur.custLogger.Info(logrus.Fields{
		"method": "GetUserIdByEmail",
		"email":  email,
	}, "Starting GetUserIdByEmail")

	accountCollection := ur.getAccountCollection()
	var existingAccount data.Account

	err := accountCollection.FindOne(ctx, bson.M{"email": email}).Decode(&existingAccount)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println(errAccount, err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetUserIdByEmail",
			"error":  err.Error(),
		}, errUser)
		return primitive.NilObjectID, err
	}
	ur.custLogger.Info(logrus.Fields{
		"method": "GetUserIdByEmail",
		"ID":     existingAccount.ID,
	}, successfulID)
	span.SetStatus(codes.Ok, successfulID)
	return existingAccount.ID, nil
}

func (ur *UserRepository) GetRoleForMagic(ctx context.Context, token string) (string, error) {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.GetUserRoleByEmail")
	defer span.End()
	ur.custLogger.Info(logrus.Fields{
		"method": "GetRoleForMagic",
		"token":  token,
	}, "Starting GetRoleForMagic")
	id := constructKeyForMagic(token)
	email, err := ur.redis.Get(id).Result()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error getting token:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetRoleForMagic",
			"error":  err.Error(),
		}, "Error finding token")
		return "", nil
	}
	accountCollection := ur.getAccountCollection()
	var existingAccount data.Account

	err = accountCollection.FindOne(ctx, bson.M{"email": email}).Decode(&existingAccount)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println(errAccount, err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetRoleForMagic",
			"error":  err.Error(),
		}, errRole)
		return "", err
	}
	span.SetStatus(codes.Ok, successfulRole)
	ur.custLogger.Info(logrus.Fields{
		"method": "GetRoleForMagic",
		"role":   existingAccount.Role,
	}, "Successfully found GetRoleForMagic")
	span.SetStatus(codes.Ok, successfulID)
	return existingAccount.Role, nil
}

func (ur *UserRepository) GetUserRoleByEmail(ctx context.Context, email string) (string, error) {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.GetUserRoleByEmail")
	defer span.End()

	accountCollection := ur.getAccountCollection()
	var existingAccount data.Account

	ur.custLogger.Info(logrus.Fields{
		"method": "GetUserRoleByEmail",
		"email":  email,
	}, "Starting GetUserRoleByEmail")

	err := accountCollection.FindOne(ctx, bson.M{"email": email}).Decode(&existingAccount)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println(errAccount, err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetUserRoleByEmail",
			"error":  err.Error(),
		}, errRole)
		return "", err
	}
	span.SetStatus(codes.Ok, successfulRole)
	ur.custLogger.Info(logrus.Fields{
		"method": "GetUserRoleByEmail",
		"role":   existingAccount.Role,
	}, successfulRole)
	return existingAccount.Role, nil
}

func (ur *UserRepository) GetUserByEmail(ctx context.Context, email string) (data.Account, error) {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.GetUserByEmail")
	defer span.End()
	accountCollection := ur.getAccountCollection()
	ur.custLogger.Info(logrus.Fields{
		"method": "GetUserByEmail",
		"email":  email,
	}, "Starting GetUserByEmail")
	var existingAccount data.Account
	err := accountCollection.FindOne(ctx, bson.M{"email": email}).Decode(&existingAccount)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println(errAccount, err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetUserByEmail",
			"error":  err.Error(),
		}, errRole)
		return data.Account{}, err
	}
	span.SetStatus(codes.Ok, "Successfully found user")
	ur.custLogger.Info(logrus.Fields{
		"method": "GetUserByEmail",
		"email":  email,
	}, "Successful GetUserByEmail")
	return existingAccount, nil
}

func (ur *UserRepository) GetUserById(ctx context.Context, id string) (data.Account, error) {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.GetUserById")
	defer span.End()
	ur.custLogger.Info(logrus.Fields{
		"method": "GetUserById",
		"ID":     id,
	}, "Starting GetUserById")
	accountCollection := ur.getAccountCollection()
	var existingAccount data.Account
	objectId, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error parsing object id:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetUserById",
			"error":  err.Error(),
		}, "Error with ID")
		return data.Account{}, err
	}
	err = accountCollection.FindOne(ctx, bson.M{"_id": objectId}).Decode(&existingAccount)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println(errAccount, err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetUserById",
			"error":  err.Error(),
		}, errUser)
		return data.Account{}, err
	}
	span.SetStatus(codes.Ok, "Successfully found user")
	ur.custLogger.Info(logrus.Fields{
		"method": "GetUserById",
		"ID":     id,
	}, "Successful GetUserById")
	return existingAccount, nil
}

func (ur *UserRepository) CheckIfPasswordIsSame(ctx context.Context, id string, password string) bool {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.CheckIfPasswordIsSame")
	defer span.End()
	ur.custLogger.Info(logrus.Fields{
		"method": "CheckIfPasswordIsSame",
		"ID":     id,
	}, "Starting CheckIfPasswordIsSame")
	acc, err := ur.GetUserById(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println(errAccount, err)
		ur.custLogger.Error(logrus.Fields{
			"method": "CheckIfPasswordIsSame",
			"error":  err.Error(),
		}, errUser)
		return false
	}
	err = bcrypt.CompareHashAndPassword([]byte(acc.Password), []byte(password))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error comparing password:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "CheckIfPasswordIsSame",
			"error":  err.Error(),
		}, "Error comparing password")
		return false
	}
	span.SetStatus(codes.Ok, "Successfully compared the passwords")
	ur.custLogger.Info(logrus.Fields{
		"method": "CheckIfPasswordIsSame",
		"ID":     id,
	}, "Successful CheckIfPasswordIsSame")
	return true
}
func (ur *UserRepository) ChangePassword(ctx context.Context, id string, password string) error {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.ChangePassword")
	defer span.End()

	accountCollection := ur.getAccountCollection()
	ur.custLogger.Info(logrus.Fields{
		"method": "ChangePassword",
		"ID":     id,
	}, "Starting ChangePassword")
	err := ForbidPassword(password)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error forbiding password:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "ChangePassword",
			"error":  err.Error(),
		}, "Error")
		return err
	}

	hashedPassword, err := hashPassword(password)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error hashing password:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "ChangePassword",
			"error":  err.Error(),
		}, "Error hashing password")
		return err
	}

	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error parsing object id:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "ChangePassword",
			"error":  err.Error(),
		}, "Error parsing object id")
		return errors.New("invalid user ID format")
	}

	filter := bson.M{"_id": objectID}
	update := bson.M{
		"$set": bson.M{"password": hashedPassword},
	}
	_, err = accountCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error updating account:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "ChangePassword",
			"error":  err.Error(),
		}, "Error updating account")
		return err
	}

	span.SetStatus(codes.Ok, successfulPassword)
	ur.custLogger.Info(logrus.Fields{
		"method": "ChangePassword",
	}, successfulPassword)
	return nil
}

func SendRecoveryEmail(userEmail, token string) error {
	recoveryURL := fmt.Sprintf("https://localhost:4200/password/recovery/%s", token)

	subject := "Password Recovery"
	body := fmt.Sprintf(`
		<html>
		<body>
			<p>Dear user,</p>
			<p>We received a request to reset your password.</p>
			<p>Please click the button below to reset your password:</p>
			<a href="%s" style="background-color: #4CAF50; color: white; padding: 10px 20px; text-align: center; text-decoration: none; display: inline-block;">Reset Password</a>
			<p>If you did not request this, please ignore this email.</p>
			<p>The link will expire in 5 minutes. </p>
			<p>Thank you!</p>
		</body>
		</html>`, recoveryURL)

	message := fmt.Sprintf("Subject: %s\r\n", subject)
	message += "MIME-Version: 1.0\r\n"
	message += "Content-Type: text/html; charset=\"UTF-8\"\r\n"
	message += "\r\n" + body

	from := os.Getenv("SMTP_EMAIL")
	password := os.Getenv("SMTP_PASSWORD")
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort := os.Getenv("SMTP_PORT")

	auth := smtp.PlainAuth("", from, password, smtpHost)

	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, from, []string{userEmail}, []byte(message))
	if err != nil {
		return fmt.Errorf("failed to send email: %v", err)
	}

	return nil
}

func (ur *UserRepository) HandleRecoveryRequest(ctx context.Context, email string) error {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.HandleRecoveryRequest")
	defer span.End()
	accountCollection := ur.getAccountCollection()
	var existingAccount data.Account
	ur.custLogger.Info(logrus.Fields{
		"method": "HandleRecoveryRequest",
		"email":  email,
	}, "Starting HandleRecoveryRequest")
	err := accountCollection.FindOne(ctx, bson.M{"email": email}).Decode(&existingAccount)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println(errAccount, err)
		ur.custLogger.Error(logrus.Fields{
			"method": "HandleRecoveryRequest",
			"error":  err.Error(),
		}, "Error finding account")
		return err
	}
	if len(existingAccount.Email) == 0 {
		span.RecordError(data.ErrEmailDoesntExist())
		span.SetStatus(codes.Error, data.ErrEmailDoesntExist().Error())
		ur.logger.Println(errAccount, data.ErrEmailDoesntExist())
		ur.custLogger.Error(logrus.Fields{
			"method": "HandleRecoveryRequest",
			"error":  data.ErrEmailDoesntExist().Error(),
		}, "Email doesn't exist")
		return data.ErrEmailDoesntExist()
	}

	newId := uuid.New().String()[:10]
	id := constructKeyForRecovery(newId)
	err = ur.redis.Set(id, email, 5*time.Minute).Err()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error saving information in database:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "HandleRecoveryRequest",
			"error":  err.Error(),
		}, "Error saving information in database")
		return err
	}
	err = SendRecoveryEmail(email, newId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error sending recovery email:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "HandleRecoveryRequest",
			"error":  err.Error(),
		}, "Error sending recovery email")
		return err
	}
	span.SetStatus(codes.Ok, "Successfully handled the request")
	ur.custLogger.Info(logrus.Fields{
		"method": "HandleRecoveryRequest",
		"email":  email,
	}, "Successful HandleRecoveryRequest")
	return nil
}

func (ur *UserRepository) ResetPassword(ctx context.Context, token string, password string) error {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.ResetPassword")
	defer span.End()
	ur.custLogger.Info(logrus.Fields{
		"method": "ResetPassword",
		"token":  token,
	}, "Starting ResetPassword")
	email, err := ur.redis.Get(constructKeyForRecovery(token)).Result()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println(errAccount, err)
		ur.custLogger.Error(logrus.Fields{
			"method": "ResetPassword",
			"error":  err.Error(),
		}, "Error finding a token")
		return err
	}

	accountCollection := ur.getAccountCollection()
	var existingAccount data.Account

	// Attempt to find the account by email
	err = accountCollection.FindOne(ctx, bson.M{"email": email}).Decode(&existingAccount)

	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			span.RecordError(errors.New(errAccount))
			span.SetStatus(codes.Error, errAccount)
			ur.logger.Println("Error: Account not found for email:", email)
			ur.custLogger.Error(logrus.Fields{
				"method": "ResetPassword",
				"error":  err.Error(),
			}, "Error finding an account")
			return errors.New(errAccount)
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println(errAccount, err)
		ur.custLogger.Error(logrus.Fields{
			"method": "ResetPassword",
			"error":  err.Error(),
		}, err.Error())
		return err
	}

	err = ForbidPassword(password)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Password forbidden:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "ResetPassword",
			"error":  err.Error(),
		}, "Password forbidden")
		return err
	}

	hashedPassword, err := hashPassword(password)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error hashing password:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "ResetPassword",
			"error":  err.Error(),
		}, "Error hashing password")
		return err
	}

	filter := bson.M{"email": email}
	update := bson.M{
		"$set": bson.M{
			"password": hashedPassword,
		},
	}
	_, err = accountCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error updating account:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "ResetPassword",
			"error":  err.Error(),
		}, "Error updating the account")
		return err
	}

	span.SetStatus(codes.Ok, successfulPassword)
	ur.custLogger.Info(logrus.Fields{
		"method": "ResetPassword",
		"token":  token,
	}, "Successful ResetPassword")
	return nil
}

func ForbidPassword(password string) error {
	file, err := os.Open("/app/10k-worst-passwords.txt")
	if err != nil {
		log.Fatal("Error opening file:", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.EqualFold(line, password) {
			return data.ErrPasswordIsNotAllowed()
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func (us *UserRepository) Delete(ctx context.Context, userID string) error {
	ctx, span := us.tracer.Start(ctx, "UserRepository.Delete")
	defer span.End()
	objectID, err := primitive.ObjectIDFromHex(userID)
	us.custLogger.Info(logrus.Fields{
		"method": "Delete",
		"ID":     userID,
	}, "Starting Delete")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		us.logger.Printf("Invalid userID format: %v", err)
		us.custLogger.Error(logrus.Fields{
			"method": "Delete",
			"error":  err.Error(),
		}, "Invalid userID format")
		return err
	}

	filter := bson.M{"_id": objectID}
	result, err := us.getAccountCollection().DeleteOne(ctx, filter)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		us.logger.Printf("Error deleting user: %v", err)
		us.custLogger.Error(logrus.Fields{
			"method": "Delete",
			"error":  err.Error(),
		}, "Error deleting user")
		return err
	}

	if result.DeletedCount == 0 {
		span.RecordError(mongo.ErrNoDocuments)
		span.SetStatus(codes.Error, mongo.ErrNoDocuments.Error())
		us.logger.Printf("No user found with ID %s", userID)
		us.custLogger.Error(logrus.Fields{
			"method": "Delete",
			"error":  mongo.ErrNoDocuments.Error(),
		}, "No user found with ID")
		return mongo.ErrNoDocuments
	}

	span.SetStatus(codes.Ok, "Successfully deleted user")
	us.logger.Printf("User with ID %s successfully deleted", userID)
	us.custLogger.Info(logrus.Fields{
		"method": "Delete",
		"ID":     userID,
	}, "Successful Delete")
	return nil
}

func (ur *UserRepository) VerifyRecaptcha(ctx context.Context, token string) (bool, error) {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.VerifyRecaptcha")
	defer span.End()
	ur.custLogger.Info(logrus.Fields{
		"method": "VerifyRecaptcha",
		"token":  token,
	}, "Starting VerifyRecaptcha")
	if token == "" {
		span.RecordError(errors.New(emptyToken))
		span.SetStatus(codes.Error, emptyToken)
		fmt.Println(emptyToken)
		ur.logger.Println("Empty reCAPTCHA token")
		ur.custLogger.Error(logrus.Fields{
			"method": "VerifyRecaptcha",
			"error":  errors.New(emptyToken),
		}, emptyToken)
		return false, errors.New("empty reCAPTCHA token")
	}

	ur.logger.Println("This is token: ", token)

	secret := os.Getenv("CAPTCHA")
	if secret == "" {
		ur.logger.Println(recaptchaNotSet)
		ur.custLogger.Warn(logrus.Fields{
			"method": "VerifyCaptcha",
		}, recaptchaNotSet)
	} else {
		ur.logger.Println("RECAPTCHA_SECRET_KEY successfully loaded")
		ur.custLogger.Info(logrus.Fields{
			"method": "VerifyCaptcha",
		}, recaptchaNotSet)
	}

	resp, err := http.PostForm("https://www.google.com/recaptcha/api/siteverify",
		url.Values{"secret": {secret}, "response": {token}})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		fmt.Println(err)
		ur.logger.Println("Error calling reCAPTCHA API:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "VerifyRecaptcha",
			"error":  err.Error(),
		}, "Error calling reCAPTCHA API")
		return false, err
	}
	defer resp.Body.Close()

	// Decode the response
	var recaptchaResp utils.RecaptchaResponse
	if err := json.NewDecoder(resp.Body).Decode(&recaptchaResp); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		fmt.Println(err)
		ur.logger.Println("Error decoding reCAPTCHA response:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "VerifyRecaptcha",
			"error":  err.Error(),
		}, "Error decoding reCAPTCHA response")
		return false, err
	}

	if !recaptchaResp.Success {
		span.RecordError(errors.New(recaptchaErr))
		span.SetStatus(codes.Error, recaptchaErr)
		fmt.Println("recaptcha failed:", recaptchaResp.ErrorCodes)
		ur.logger.Println("reCAPTCHA verification failed:", recaptchaResp.ErrorCodes)
		ur.custLogger.Error(logrus.Fields{
			"method": "VerifyRecaptcha",
			"error":  errors.New(recaptchaErr),
		}, recaptchaErr)
		return false, errors.New("reCAPTCHA verification failed")
	}

	span.SetStatus(codes.Ok, "Successfully verified reCAPTCHA token")
	ur.custLogger.Info(logrus.Fields{
		"method": "VerifyRecaptcha",
		"token":  token,
	}, "Successful VerifyRecaptcha")
	return true, nil
}

func (ur *UserRepository) GetUsersByIds(ctx context.Context, ids []string) ([]data.Account, error) {
	ctx, span := ur.tracer.Start(ctx, "UserRepository.GetUsersByIds")
	defer span.End()
	ur.custLogger.Info(logrus.Fields{
		"method": "GetUsersByIds",
		"ids":    len(ids),
	}, "Starting GetUsersByIds")
	accountCollection := ur.getAccountCollection()

	var objectIds []primitive.ObjectID
	for _, id := range ids {
		objectId, err := primitive.ObjectIDFromHex(id)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			ur.logger.Println("Error parsing ObjectID:", err)
			ur.custLogger.Error(logrus.Fields{
				"method": "GetUsersByIds",
				"error":  err.Error(),
			}, "Error parsing ObjectID")
			return nil, fmt.Errorf("invalid user ID format: %s", id)
		}
		objectIds = append(objectIds, objectId)
	}

	filter := bson.M{"_id": bson.M{"$in": objectIds}}
	cursor, err := accountCollection.Find(ctx, filter)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error finding accounts:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetUsersByIds",
			"error":  err.Error(),
		}, "Error finding accounts")
		return nil, err
	}
	defer cursor.Close(ctx)

	var users []data.Account
	if err := cursor.All(ctx, &users); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ur.logger.Println("Error decoding accounts:", err)
		ur.custLogger.Error(logrus.Fields{
			"method": "GetUsersByIds",
			"error":  err.Error(),
		}, "Error decoding")
		return nil, err
	}

	span.SetStatus(codes.Ok, "Successfully retrieved users")
	ur.custLogger.Info(logrus.Fields{
		"method": "GetUsersByIds",
		"users":  len(users),
	}, "Successful GetUsersByIds")
	return users, nil
}
