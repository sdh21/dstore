package FileServer

import (
	"crypto/sha512"
	"encoding/base64"
	"github.com/gin-gonic/gin"
	"github.com/sdh21/dstore/kvstore"
	"net/http"
	"regexp"
	"sync"
	"time"
)

func SessionMiddleware(auth *AuthenticationService) gin.HandlerFunc {
	return func(c *gin.Context) {
		token, err := c.Cookie("session-token")

		// for every function call, we either restore the session ID from cookie,
		// or create a new one.

		createNewSession := false
		if err != nil {
			createNewSession = true
		} else {
			sessionId, ok := auth.ts.parseCookieValue(token)
			if !ok {
				createNewSession = true
			} else {
				c.Set("sessionId", sessionId)
			}
		}
		if createNewSession {
			sessionId, token, err := auth.ts.issueCookieValue("")
			if err != nil {
				c.AbortWithStatus(http.StatusInternalServerError)
				return
			}
			auth.ts.createSession(sessionId)

			c.SetCookie("session-token", token, 7200, "/",
				"", false /* TODO: change to true */, true)
			c.Set("sessionId", sessionId)
		}
	}
}

func AuthMiddleware(auth *AuthenticationService) gin.HandlerFunc {
	return func(c *gin.Context) {
		sessionId, err := c.Cookie("session-token")
		if err != nil {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		userToken, err := c.Cookie("user-token")
		if err != nil {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		csrfToken := c.PostForm("csrf-token")
		if csrfToken == "" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		userData, found := auth.ts.getSessionFromCookie(sessionId)
		if !found {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		authPassed := true

		userData.lock.RLock()
		if userToken != userData.userToken || csrfToken != userData.csrfToken {
			authPassed = false
		}
		userId := userData.userId
		userData.lock.RUnlock()

		if !authPassed {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		c.Set("userId", userId)
	}
}

var usernameRequirement = "^[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)*$"

type AuthenticationService struct {
	ts            *TemporaryStorage
	mu            sync.Mutex
	usernameRegex *regexp.Regexp
}

func NewAuthService(ts *TemporaryStorage) *AuthenticationService {
	var err error
	auth := &AuthenticationService{}
	auth.ts = ts
	auth.usernameRegex, err = regexp.Compile(usernameRequirement)
	if err != nil {
		panic(err)
	}
	return auth
}

// ---------------------------------------------
// Register and Login

func (auth *AuthenticationService) isValidUsername(username string) bool {
	if len(username) < 32 && len(username) > 0 && auth.usernameRegex.MatchString(username) {
		return true
	} else {
		return false
	}
}

func (auth *AuthenticationService) isValidPassword(password string) bool {
	if len(password) < 128 && len(password) > 0 {
		return true
	} else {
		return false
	}
}

func (auth *AuthenticationService) Register() gin.HandlerFunc {
	return func(c *gin.Context) {
		username := c.PostForm("username")
		password := c.PostForm("password")

		if !auth.isValidUsername(username) {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "invalid username",
			})
			return
		}

		if !auth.isValidPassword(password) {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "invalid password",
			})
			return
		}

		digest := sha512.Sum512([]byte(password))
		result := auth.ts.db.Submit(&kvstore.Transaction{
			ClientId:      "",
			TransactionId: 0,
			TimeStamp:     time.Now().Unix(),
			CollectionId:  username,
			Ops: []*kvstore.AnyOp{
				kvstore.OpMapStore([]string{}, "password", base64.StdEncoding.EncodeToString(digest[:])).
					OpSetTableOption(kvstore.CreateTableOption_UseTransactionTableId, true),
				kvstore.OpMapStore([]string{}, "files", map[string]*kvstore.AnyValue{}),
			},
			TableVersionExpected: -1,
		})
		if result == nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if result.Status == kvstore.TransactionResult_TableAlreadyExists {
			c.JSON(http.StatusOK, gin.H{
				"ok":      false,
				"message": "User already exists",
			})
			return
		} else if result.Status == kvstore.TransactionResult_OK {
			c.JSON(http.StatusOK, gin.H{
				"ok":      true,
				"message": "Success",
			})
			return
		} else {
			c.JSON(http.StatusOK, gin.H{
				"ok":      false,
				"message": "TransactionResult: " + result.Status.String(),
			})
			return
		}
	}
}

func (auth *AuthenticationService) Login() gin.HandlerFunc {
	return func(c *gin.Context) {
		username := c.PostForm("username")
		password := c.PostForm("password")

		if !auth.isValidUsername(username) {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "invalid username",
			})
			return
		}

		if !auth.isValidPassword(password) {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"Message": "invalid password",
			})
			return
		}

		digest := sha512.Sum512([]byte(password))
		result := auth.ts.db.Submit(&kvstore.Transaction{
			ClientId:      "",
			TransactionId: 0,
			TimeStamp:     time.Now().Unix(),
			CollectionId:  username,
			Ops: []*kvstore.AnyOp{
				kvstore.OpGet([]string{"password"}, "p"),
			},
			TableVersionExpected: -1,
		})
		if result == nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if result.Status == kvstore.TransactionResult_TableNotExists {
			c.JSON(http.StatusOK, gin.H{
				"ok":      false,
				"message": "User not found",
			})
			return
		}
		if result.Status != kvstore.TransactionResult_OK {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		p, found := result.Values["p"]
		if !found {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if p.IsNull {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		if p.Value.Str != base64.StdEncoding.EncodeToString(digest[:]) {
			c.JSON(http.StatusOK, gin.H{
				"ok":      false,
				"message": "Wrong password",
			})
			return
		}
		session := c.MustGet("sessionId").(string)
		_, csrfToken, err := auth.ts.issueCookieValue(username)
		if err != nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		_, userToken, err := auth.ts.issueCookieValue(username)
		if err != nil {
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		sessionData := auth.ts.retrieveSession(session)
		sessionData.lock.Lock()
		sessionData.csrfToken = csrfToken
		sessionData.userToken = userToken
		sessionData.userId = username
		sessionData.lock.Unlock()

		c.SetSameSite(http.SameSiteStrictMode)
		c.SetCookie("user-token", userToken, 7200, "/",
			"", false /*  FIX: change to true  */, true)

		c.JSON(http.StatusOK, gin.H{
			"ok":         true,
			"message":    "OK",
			"csrf-token": csrfToken,
		})
	}
}
