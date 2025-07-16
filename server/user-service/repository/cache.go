package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/crypto/bcrypt"
	"log"
	"main.go/data"
	"main.go/utils"
	"net/mail"
	"net/smtp"
	"os"
	"regexp"
	"time"
)

type UserCache struct {
	cli            *redis.Client
	log            *log.Logger
	userRepository *UserRepository
	tracer         trace.Tracer
}

const (
	cacheRequestConstruct  = "requests:%s"
	cacheUserConstruct     = "activeUser:%s"
	cacheMagicConstruct    = "magic:%s"
	cacheRegisterConstruct = "register:%s"
	cacheMagic             = "magic"
	cacheRequests          = "requests"
	cacheUser              = "activeUser"
	cacheRegister          = "register"
	signError              = "Unexpected signing method"
	invalidToken           = "invalid token"
)

func constructKeyForRequest(id string) string {
	return fmt.Sprintf(cacheRequestConstruct, id)
}

func constructKeyForUser(id string) string {
	return fmt.Sprintf(cacheUserConstruct, id)
}

func constructKeyForMagic(email string) string {
	return fmt.Sprintf(cacheMagicConstruct, email)
}

func constructKeyForRegister(email string) string { return fmt.Sprintf(cacheRegister, email) }

func NewCache(logger *log.Logger, repo *UserRepository, trace trace.Tracer) (*UserCache, error) {
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisAddress := fmt.Sprintf("%s:%s", redisHost, redisPort)

	client := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})

	if err := client.Ping().Err(); err != nil {
		return nil, err
	}

	return &UserCache{
		cli:            client,
		log:            logger,
		userRepository: repo,
		tracer:         trace,
	}, nil
}

func (uc *UserCache) Login(ctx context.Context, user *data.LoginCredentials, token string) error {
	ctx, span := uc.tracer.Start(ctx, "Cache.Login")
	defer span.End()
	keyForId, err := uc.userRepository.GetUserIdByEmail(ctx, user.Email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return errors.New("key is not found")
	}

	userFound, err := uc.userRepository.GetUserByEmail(ctx, user.Email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	err = bcrypt.CompareHashAndPassword([]byte(userFound.Password), []byte(user.Password))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return errors.New("email and password don't match")
	}

	value, err := json.Marshal(token)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	err = uc.cli.Set(constructKeyForUser(keyForId.Hex()), value, 1*time.Hour).Err()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.SetStatus(codes.Ok, "Login success")

	return err
}

func (uc *UserCache) VerifyToken(ctx context.Context, userID string) (bool, error) {
	_, span := uc.tracer.Start(ctx, "Cache.VerifyToken")
	defer span.End()
	key := constructKeyForUser(userID)
	exists, err := uc.cli.Exists(key).Result()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, err
	}

	if exists > 0 {
		span.SetStatus(codes.Ok, "Token exists")
		return true, nil
	} else {
		span.SetStatus(codes.Ok, "Token not found")
		return false, nil
	}
}

func (uc *UserCache) Logout(ctx context.Context, id string) error {
	_, span := uc.tracer.Start(ctx, "Cache.Logout")
	defer span.End()
	key := constructKeyForUser(id)
	err := uc.cli.Del(key).Err()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "Logout success")
	return nil
}

func (uc *UserCache) GetUserIDFromToken(ctx context.Context, token string) (string, error) {
	_, span := uc.tracer.Start(ctx, "Cache.GetUserIDFromToken")
	defer span.End()
	parsedToken, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			span.RecordError(errors.New(signError))
			span.SetStatus(codes.Error, signError)
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(os.Getenv("SECRET_KEY")), nil
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uc.log.Println("Error parsing token:", err)
		return "", errors.New(invalidToken)
	}

	if claims, ok := parsedToken.Claims.(jwt.MapClaims); ok && parsedToken.Valid {
		userID := claims["user_id"].(string)
		span.SetStatus(codes.Ok, "User found")
		return userID, nil
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, invalidToken)
	return "", errors.New("invalid token or missing user ID")
}

func SendMagicLink(id, userEmail string) error {
	recoveryURL := fmt.Sprintf("https://localhost:4200/magic/%s", id)

	subject := "Magic Link"
	body := fmt.Sprintf(`
		<html>
		<body>
			<p>Dear user,</p>
			<p>We received a request for a magic link.</p>
			<p>Please click the button below to log in without the password:</p>
			<a href="%s" style="background-color: #4CAF50; color: white; padding: 10px 20px; text-align: center; text-decoration: none; display: inline-block;">Abracadabra</a>
			<p>The link expires in 5 minutes.</p>
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

func (c *UserCache) ImplementMagic(ctx context.Context, email string) error {
	ctx, span := c.tracer.Start(ctx, "Cache.ImplementMagic")
	defer span.End()
	accountCollection := c.userRepository.getAccountCollection()
	var existingAccount data.Account

	err := accountCollection.FindOne(ctx, bson.M{"email": email}).Decode(&existingAccount)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.log.Println("Error finding account:", err)
		return err
	}

	newId := uuid.New().String()[:10]

	id := constructKeyForMagic(newId)
	err = c.cli.Set(id, existingAccount.Email, 5*time.Minute).Err()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.log.Println("Error setting token:", err)
		return err
	}
	err = SendMagicLink(newId, email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.log.Println("Error sending magic link:", err)
		return err
	}
	span.SetStatus(codes.Ok, "Successfully sent a magic link.")
	return nil
}

func (c *UserCache) VerifyMagic(ctx context.Context, url string) (string, string, error) {
	ctx, span := c.tracer.Start(ctx, "Cache.VerifyMagic")
	defer span.End()

	id := constructKeyForMagic(url)
	email, err := c.cli.Get(id).Result()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.log.Println("Error getting token:", err)
		return "", "", err
	}
	accountCollection := c.userRepository.getAccountCollection()
	var existingAccount data.Account
	err = accountCollection.FindOne(ctx, bson.M{"email": email}).Decode(&existingAccount)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.log.Println("Error finding account:", err)
		return "", "", err
	}

	c.log.Println("email is " + email + "role is " + existingAccount.Role + "user id is " + existingAccount.ID.Hex())
	token, err := utils.CreateToken(email, existingAccount.Role, existingAccount.ID.Hex())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.log.Println("Error creating token:", err)
		return "", "", err
	}
	c.log.Println("token is created " + token)
	err = c.cli.Set(constructKeyForUser(existingAccount.ID.Hex()), token, 5*time.Minute).Err()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.log.Println("Error setting token:", err)
		return "", "", err
	}

	span.SetStatus(codes.Ok, "Successfully verified magic link.")
	return existingAccount.ID.Hex(), token, nil
}

func (uc *UserCache) VerifyTokenWithUserId(ctx context.Context, token string) (string, error) {
	ctx, span := uc.tracer.Start(ctx, "Cache.VerifyTokenWithUserId")
	defer span.End()
	userID, err := uc.GetUserIDFromToken(ctx, token)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	key := constructKeyForUser(userID)
	exists, err := uc.cli.Exists(key).Result()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}

	if exists == 0 {
		span.RecordError(errors.New("user not found"))
		span.SetStatus(codes.Error, "user not found")
		return "", errors.New("invalid token or user not found")
	}

	span.SetStatus(codes.Ok, "Successfully verified token.")
	return userID, nil
}

func (uc *UserCache) GetUserRole(ctx context.Context, token string) (string, error) {
	_, span := uc.tracer.Start(ctx, "Cache.GetUserRole")
	defer span.End()
	userRole, err := uc.GetRoleFromToken(ctx, token)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	span.SetStatus(codes.Ok, "Successfully retrieved user role.")

	return userRole, nil
}

func (uc *UserCache) GetRoleFromToken(ctx context.Context, token string) (string, error) {
	_, span := uc.tracer.Start(ctx, "Cache.GetRoleFromToken")
	defer span.End()
	parsedToken, err := jwt.Parse(token, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			span.RecordError(errors.New(signError))
			span.SetStatus(codes.Error, signError)
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(os.Getenv("SECRET_KEY")), nil
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uc.log.Println("Error parsing token:", err)
		return "", errors.New(invalidToken)
	}

	if claims, ok := parsedToken.Claims.(jwt.MapClaims); ok && parsedToken.Valid {
		userRole := claims["role"].(string)
		span.SetStatus(codes.Ok, "Role has been successfully retrieved.")
		return userRole, nil
	}
	span.RecordError(errors.New(invalidToken))
	span.SetStatus(codes.Error, invalidToken)

	return "", errors.New("role not found in token")
}

func (uc *UserCache) Register(ctx context.Context, request *data.AccountRequest) error {
	ctx, span := uc.tracer.Start(ctx, "UserRepository.Registration")
	defer span.End()

	var existingAccount data.Account
	err := uc.userRepository.getAccountCollection().FindOne(ctx, bson.M{"email": request.Email}).Decode(&existingAccount)
	if err == nil {
		span.SetStatus(codes.Error, "Account already exists.")
		return errors.New("account already exists")
	}

	construct := constructKeyForRegister(request.Email)
	_, err = uc.cli.Get(construct).Result()
	if errors.Is(err, redis.Nil) {
		span.AddEvent("Redis key not found, proceeding with registration")
	} else if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	} else {
		span.SetStatus(codes.Error, "Registration already in progress")
		return errors.New("registration already in progress")
	}

	if _, err = mail.ParseAddress(request.Email); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if len(request.FirstName) == 0 {
		return errors.New("first name is empty")
	}
	if len(request.LastName) == 0 {
		return errors.New("last name is empty")
	}
	if len(request.Role) == 0 {
		return errors.New("role is empty")
	}
	if err = ValidatePassword(request.Password); err != nil {
		return err
	}

	hashedPassword, err := hashPassword(request.Password)
	if err != nil {
		return err
	}

	account := &data.Account{
		Email:     request.Email,
		FirstName: request.FirstName,
		LastName:  request.LastName,
		Password:  hashedPassword,
		Role:      request.Role,
	}

	// Serialize the account to JSON
	accountJSON, err := json.Marshal(account)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to serialize account")
		return err
	}

	// Store the serialized account in Redis
	err = uc.cli.Set(construct, accountJSON, 5*time.Minute).Err()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	// Send verification email
	err = sendEmail(account.Email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	span.SetStatus(codes.Ok, "Registration successful")
	return nil
}

func (uc *UserCache) VerifyAccount(ctx context.Context, email string) error {
	ctx, span := uc.tracer.Start(ctx, "UserRepository.VerifyAccount")
	defer span.End()

	construct := constructKeyForRegister(email)

	val, err := uc.cli.Get(construct).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return fmt.Errorf("account verification key not found in Redis for email: %s", email)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("Redis error: %v", err))
		return fmt.Errorf("failed to get account verification data from Redis: %w", err)
	}

	var account data.Account
	err = json.Unmarshal([]byte(val), &account)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("JSON unmarshal error: %v", err))
		return fmt.Errorf("failed to unmarshal account data for email: %s, error: %w", email, err)
	}

	existingAccount := data.Account{}
	err = uc.userRepository.getAccountCollection().FindOne(ctx, bson.M{"email": account.Email}).Decode(&existingAccount)
	if err == nil {
		span.RecordError(errors.New("Account already exists in the database"))
		span.SetStatus(codes.Error, "Account already exists")
		return fmt.Errorf("account already exists for email: %s", account.Email)
	}

	_, err = uc.userRepository.getAccountCollection().InsertOne(ctx, account)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("Database insert error: %v", err))
		return fmt.Errorf("failed to insert account into the database: %w", err)
	}

	span.SetStatus(codes.Ok, "Account verified successfully")
	return nil
}

func ValidatePassword(password string) error {
	rules := map[string]string{
		"length":      `.{8,}`,
		"uppercase":   `[A-Z]`,
		"lowercase":   `[a-z]`,
		"number":      `[0-9]`,
		"specialChar": `[!@#$%^&*()]`,
	}

	messages := map[string]string{
		"length":      "Password must be at least 8 characters long.",
		"uppercase":   "Password must contain at least one uppercase letter.",
		"lowercase":   "Password must contain at least one lowercase letter.",
		"number":      "Password must contain at least one number.",
		"specialChar": "Password must contain at least one special character.",
	}

	var validationErrors []string

	for rule, pattern := range rules {
		matched, err := regexp.MatchString(pattern, password)
		if err != nil {
			return fmt.Errorf("error validating password for rule %s: %w", rule, err)
		}
		if !matched {
			validationErrors = append(validationErrors, messages[rule])
		}
	}
	if len(validationErrors) > 0 {
		return errors.New(fmt.Sprintf("Password validation failed:\n%s", validationErrors))
	}
	return nil
}
